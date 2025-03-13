from fastapi import FastAPI,HTTPException,WebSocket
import json
import redis
from fastapi import FastAPI
from pydantic import BaseModel
import firebase_admin.auth as auth
from firebase_config import db
import asyncio
from aiokafka import AIOKafkaConsumer,AIOKafkaProducer
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
import firebase_admin
from firebase_admin import auth, firestore
from math import sqrt,radians,sin,cos,atan2
app = FastAPI()
redis_client = redis.StrictRedis(host="localhost", port=6379, db=0, decode_responses=True)


# Khởi tạo Firebase
if not firebase_admin._apps:
    firebase_admin.initialize_app()

db = firestore.client()
@app.get("/")
async def root():
    return {"message": "Hello World"}

producer = None
async def init_kafka_producer():
    global producer
    producer = AIOKafkaProducer(
        bootstrap_servers="localhost:9092",
        value_serializer=lambda v: json.dumps(v).encode("utf-8")  # Serialize JSON
    )
    await producer.start()

# Danh sách kết nối WebSocket của tài xế
active_drivers = {}
driver_id_request={}

def haversine(lat1, lon1, lat2, lon2):
    R = 6371  # Bán kính Trái Đất (km)
    lat1, lon1, lat2, lon2 = map(radians, [lat1, lon1, lat2, lon2])
    dlat = lat2 - lat1
    dlon = lon2 - lon1
    a = sin(dlat/2)**2 + cos(lat1) * cos(lat2) * sin(dlon/2)**2
    c = 2 * atan2(sqrt(a), sqrt(1-a))
    return R * c  # Khoảng cách (km)
def find_nearest_driver(lat, lon):
    nearest_driver = None
    min_distance = float("inf") # Nếu trả về True thì Redis đang chạy
    for key in redis_client.keys("driver:*"):  # Lấy tất cả tài  xế trong Redis
        driver_id = key.split(":")[1]

        # Kiểm tra trạng thái tài xế (bận hay rảnh)
        driver_status = redis_client.get(f"status:{driver_id}")
        if driver_status and driver_status == "busy" and driver_id not in connected_clients:
            continue  # Bỏ qua tài xế đang bận

        data = json.loads(redis_client.get(key))
        driver_lat, driver_lon = data["latitude"], data["longitude"]   
        distance = haversine(lat, lon, driver_lat, driver_lon)
        if distance < min_distance:
            min_distance = distance
            nearest_driver = driver_id
    return nearest_driver

class RideRequest(BaseModel):
    request_id:str
    user_id: str
    latStart: float
    lonStart: float
    latEnd: float
    lonEnd: float
@app.post("/request_ride/")
async def request_ride(request: RideRequest): 
    nearest_driver = find_nearest_driver(request.latStart, request.lonStart)
    
    if not nearest_driver:
        return {"message": "Không có tài xế khả dụng!"}
    message = {
        "request_id":request.request_id,
        "user_id": request.user_id,
        "latStart":request.latStart ,
        "lonStart":request.lonStart,
        "latEnd":request.latEnd,
        "lonEnd":request.lonEnd,
        "driver_id":nearest_driver,
    }
    #Luu websocket
    driver_id_request['driver_id']=nearest_driver
    redis_client.set(f"connect:{request.user_id}",nearest_driver )
    redis_client.set(f"status:{nearest_driver}", "busy") 
    redis_client.set(f"status_customer:{request.user_id}", "have_ride") 
    redis_client.set(f"ride:{nearest_driver}",request.user_id)
    # print(redis_client.get(f"status:{nearest_driver}"))
    redis_client.set(f"ride_end:{nearest_driver}", json.dumps({"latEnd": request.latEnd, "lonEnd": request.lonEnd}))
    await producer.send("ride_requests", message)  # Gửi JSON string lên Kafka
    print("Đã gửi",message)
    return {"message": "Yêu cầu đặt xe đã gửi", "driver_id": nearest_driver}

@app.get("/get-driver/{customer_id}")
async def get_driver(customer_id: str):
    """API cho khách hàng lấy driver_id sau khi đặt xe"""
    driver_id = redis_client.get(f"connect:{customer_id}")
    #kiểm tra khách hàng đó có chuyến đi chưa
    status_customer = redis_client.get(f"status_customer:{customer_id}")
    if driver_id:
        return {"customer_id": customer_id, "driver_id": driver_id,"status_customer":status_customer}
    return {"message": "Chưa có tài xế nhận chuyến!"}
class RegisterRequest(BaseModel):
    email: str
    password: str
    sdt:str
    name:str
    user_type: str  # "khachhang" hoặc "taixe"
@app.post("/register/")
async def register_user(request: RegisterRequest):
    try:
        # Tạo tài khoản trên Firebase Auth
        user = auth.create_user(email=request.email, password=request.password)
        # Tạo ID: KH_xxxx hoặc TX_xxxx
        user_count = len(db.collection("users").get()) + 1
        user_id = f"{'KH' if request.user_type == 'khachhang' else 'TX'}_{user_count:04d}"

        # Lưu vào Firestore
        user_data = {
            "id": user_id,
            "email": request.email,
            "user_type": request.user_type,
            "name":request.name,
            "sdt":request.sdt
        }
        driver_data = {
            "id": user_id,
            "email": request.email,
            "user_type": "taixe",
            "name":request.name,
            "sdt":request.sdt,
            "status":"available",
            "lat":0,
            "lon":0,
        }
        #lưu để chính người dùng truy cập
        db.collection("users").document(user.uid).set(user_data)
        #lưu để quản trị hệ thống truy cập
        if request.user_type=="khachhang":
            db.collection("customers").document(user_id).set(user_data)
        else:
            db.collection("drivers").document(user_id).set(driver_data)
        return {"message": "Tài khoản đã được tạo", "user_id": user_id}

    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

@app.post("/update_location/")
async def update_location(data: dict):
    driver_id = data.get("driver_id")
    latitude = data.get("latitude")
    longitude = data.get("longitude")

    # Lưu vị trí tài xế vào Redis hỗ trợ tim tài xế gần nhất
    redis_client.set(f"driver:{driver_id}", json.dumps({"latitude": latitude, "longitude": longitude}))
    # Gửi dữ liệu vào Kafka topic "driver_location"
    await producer.send("driver_location", {
        "driver_id": driver_id,
        "latitude": latitude,
        "longitude": longitude,
    })

    return {"message": "Location sent to Kafka"}

@app.post("/complete_ride/")
async def complete_ride(driver_id: str):
    # Kiểm tra tài xế có chuyến đi nào đang hoạt động không
    ride_info = redis_client.get(f"ride:{driver_id}")
    
    if not ride_info:
        return {"message": "Không có chuyến đi nào đang hoạt động!"}
    # Cập nhật trạng thái tài xế
    redis_client.set(f"status:{driver_id}", "available")
    redis_client.set(f"status_customer:{ride_info}", "no_ride") 
    return {"message": "Chuyến đi đã hoàn thành!"}
# OPEN khi mà tài xế đăng nhập, phục vụ nhận thông báo chuyến đi
@app.websocket("/ws/{driver_id}")
async def websocket_endpoint(websocket: WebSocket, driver_id: str):
    await websocket.accept()
    active_drivers[driver_id] = websocket
    try:
        while True:
            await websocket.receive_text()  # Lắng nghe dữ liệu từ client
    except WebSocketDisconnect:
        del active_drivers[driver_id]

# Danh sách WebSocket kết nối của khách hàng
#Open khi mà khách hàng gửi request, phục vụ nhận thông báo vị trí tài xế
connected_clients = {} 
@app.websocket("/ws/{customer_id}/{driver_id}")
async def websocket_endpoint(websocket: WebSocket, customer_id: str, driver_id: str):
    await websocket.accept()

    # Lưu kết nối WebSocket vào danh sách theo dõi tài xế
    if driver_id not in connected_clients:
        connected_clients[driver_id] = []
    connected_clients[driver_id].append(websocket)

    try:
        while True:
            await websocket.receive_text()  # Chờ tin nhắn từ client (nếu có)
    except WebSocketDisconnect:
        connected_clients[driver_id].remove(websocket)
        if not connected_clients[driver_id]:  # Nếu không còn ai theo dõi tài xế, xóa khỏi danh sách
            del connected_clients[driver_id]

async def consume_ride_requests():
    consumer = AIOKafkaConsumer(
    "ride_requests",
    bootstrap_servers="localhost:9092",
    value_deserializer=lambda x: json.loads(x.decode("utf-8")),
    auto_offset_reset="latest"
)
    await consumer.start()
    try:
        async for msg in consumer:
            data = msg.value
            driver_id = data.get("driver_id")
            print(data)
            if driver_id in active_drivers:
                websocket = active_drivers[driver_id]
                try:
                    await websocket.send_text(json.dumps(data))  # Gửi thông tin cuốc xe cho tài xế
                except Exception:
                    del active_drivers[driver_id]  # Xóa tài xế nếu mất kết nối
    finally:
        await consumer.stop()


async def consume_driver_location():
    consumer = AIOKafkaConsumer(
        "driver_location",
        bootstrap_servers="localhost:9092",
        group_id="location_group"
    )
    await consumer.start()
    try:
        async for msg in consumer:
            data = json.loads(msg.value.decode("utf-8"))
            driver_id = data.get("driver_id")
            latitude = data.get("latitude")
            longitude = data.get("longitude")
            # Nếu có khách hàng đang theo dõi driver_id này, gửi WebSocket
            if driver_id in connected_clients:
                message = json.dumps({"latitude": latitude, "longitude": longitude})
                print(message)
                for ws in connected_clients[driver_id]:
                    await ws.send_text(message)
    finally:
        await consumer.stop()

@app.on_event("startup")
async def start_kafka_consumer():
    asyncio.create_task(consume_ride_requests())
    asyncio.create_task(consume_driver_location())
@app.on_event("startup")
async def startup_event():
    await init_kafka_producer()

@app.on_event("shutdown")
async def shutdown_event():
    await producer.stop()

@app.on_event("startup")
def reset_redis_status():
    """
    Reset lại trạng thái của khách hàng và tài xế khi FastAPI khởi động.
    """
    # Lấy danh sách tất cả các khóa liên quan đến status
    customer_keys = redis_client.keys("status_customer:*")
    driver_keys = redis_client.keys("status:*")

    # Reset trạng thái khách hàng về "no_ride"
    for key in customer_keys:
        redis_client.set(key, "no_ride")

    # Reset trạng thái tài xế về "available"
    for key in driver_keys:
        redis_client.set(key, "available")

    print("✅ Đã reset trạng thái Redis khi khởi động FastAPI")
    print(redis_client.get("status_customer:*"))
