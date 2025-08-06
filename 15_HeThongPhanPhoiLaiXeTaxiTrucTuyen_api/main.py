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
import time


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
def find_nearest_driver(lat, lon,decline_driver=0):
    nearest_driver = None
    start_time = time.time()  
    min_distance = float("inf") 
    for key in redis_client.keys("driver:*"):  
        driver_id = key.split(":")[1]
        #TASK TRACKER KIỂM TRA TRẠNG THÁI NGƯỜI DÙNG
        driver_status = redis_client.get(f"status_driver:{driver_id}")
        if driver_status == "busy" and active_drivers.get(driver_id) is None:
            continue  

        # TASK TRACKER LẤY VỊ TRÍ TÀI XẾ, ĐỂ TÌM TÀI XẾ PHÙ HỢP
        data = json.loads(redis_client.get(key))
        driver_lat, driver_lon = data["latitude"], data["longitude"]   
        distance = haversine(lat, lon, driver_lat, driver_lon)
        if distance < min_distance:
            min_distance = distance
            nearest_driver = driver_id
    end_time = time.time() 
    elapsed_time = end_time - start_time  # Tính thời gian chạy

    print(f"Thời gian phản hồi cho: {elapsed_time:.4f} giây")
    return nearest_driver

class RideRequest(BaseModel):
    request_id:str
    user_id: str
    latStart: float
    lonStart: float
    latEnd: float
    lonEnd: float
    name:str
@app.post("/request_ride/")
async def request_ride(request: RideRequest): 
    nearest_driver = find_nearest_driver(request.latStart, request.lonStart)
    start_time = time.time()
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
        "name":request.name
    }
    #Luu websocket
    driver_id_request['driver_id']=nearest_driver
    redis_client.set(f"connect:{request.user_id}",nearest_driver )
    redis_client.set(f"request_id:{nearest_driver}",request.request_id )

    

    redis_client.set(f"ride:{nearest_driver}",request.user_id)
    # TASK TRACKER GỬI YÊU CẦU 
    await producer.send("ride_requests", message) 
    print("Đã gửi",message)
    end_time = time.time()  # Kết thúc đo
    elapsed_time = end_time - start_time 
    print(f"Thời gian gửi message cho yêu cầu cuốc xe là: {elapsed_time:.4f} giây")
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
    user_type: str
#TASK TRACKER THÊM NGƯỜI DÙNG
@app.post("/register/")
async def register_user(request: RegisterRequest):
    try:
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
            "sdt":request.sdt,
            "status":"no_ride"
        }
        driver_data = {
            "id": user_id,
            "email": request.email,
            "user_type": "taixe",
            "name":request.name,
            "sdt":request.sdt,
            "status":"available"
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


#TASK TRACKER XEM LỊCH SỬ CHUYẾN ĐI
@app.get("/rides/{id_customer}")
async def get_rides(id_customer: str):
    start_time = time.time() 
    rides_ref = db.collection("rides").where("idCustomer", "==", id_customer)
    docs = rides_ref.stream()
    
    rides = []
    for doc in docs:
        rides.append(doc.to_dict())
        print(doc.to_dict())

    if not rides:
        raise HTTPException(status_code=404, detail="Không tìm thấy lịch sử chuyến đi")
    end_time = time.time()  
    elapsed_time = end_time - start_time 
    print(f"Thời gian tra lịch sử tìm kiếm là: {elapsed_time:.4f} giây")

    return {"rides": rides}

# TASK TRACKER TÌM KIẾM
@app.get("/search_driver/")
async def search_driver(name: str):
    start_time = time.time()  
    try:
        drivers_ref = db.collection("drivers")
        query = drivers_ref.stream()
        
        keyword_lower = name.lower()  
        results = []

        for doc in query:
            driver_data = doc.to_dict()
            driver_name_lower = driver_data.get("name", "").lower()  
            
            if keyword_lower in driver_name_lower:
                results.append(driver_data)

        if not results:
            raise HTTPException(status_code=404, detail="Không tìm thấy tài xế")
        end_time = time.time()  
        elapsed_time = end_time - start_time 
        print(f"Thời gian tra cứu tài xế là: {elapsed_time:.4f} giây")
        return {"drivers": results}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    

    
#TASK TRACKER THÊM TỌA ĐỘ
@app.post("/update_location/")
async def update_location(data: dict):
    start_time = time.time()
    driver_id = data.get("driver_id")
    latitude = data.get("latitude")
    longitude = data.get("longitude")
    # Lưu vị trí tài xế vào Redis hỗ trợ tim tài xế gần nhất
    redis_client.set(f"driver:{driver_id}", json.dumps({"latitude": latitude, "longitude": longitude}))
    # Gửi dữ liệu vào Kafka topic "driver_location"
    driver_data_lat_lon = {
        "id": driver_id,
        "latitude": latitude,
        "longitude": longitude
    }
    await producer.send("driver_location", driver_data_lat_lon)
    # lưu vị trí tài xế vào FB
    db.collection("drivers").document(driver_id).update(driver_data_lat_lon)
    end_time = time.time()  # Kết thúc đo
    elapsed_time = end_time - start_time  # Tính thời gian gửi
    print(f"Thời gian gửi vị trí tài xế real-time: {elapsed_time:.4f} giây")
    return {"message": "Location sent to Kafka"}

@app.post("/complete_ride/")
async def complete_ride(driver_id: str):
    # Kiểm tra tài xế có chuyến đi nào đang hoạt động (với khách hàng nào)
    cusromer_id = redis_client.get(f"ride:{driver_id}")
    
    if not cusromer_id:
        return {"message": "Không có chuyến đi nào đang hoạt động!"}
    # Cập nhật trạng thái tài xế
    db.collection("customers").document(cusromer_id).update({"status":"no_ride"})
    db.collection("drivers").document(driver_id).update({"status":"available"})
    redis_client.set(f"status_driver:{driver_id}", "available") 
    redis_client.set(f"status_customer:{cusromer_id}", "no_ride")
    return {"message": "Chuyến đi đã hoàn thành!"}
# OPEN khi mà tài xế đăng nhập, phục vụ nhận thông báo chuyến đi
@app.websocket("/ws/{driver_id}")
async def websocket_endpoint(websocket: WebSocket, driver_id: str):
    await websocket.accept()
    active_drivers[driver_id] = websocket
    print(active_drivers)
    try:
        while True:
            await websocket.receive_text()  # Lắng nghe dữ liệu từ client
    except WebSocketDisconnect:
        del active_drivers[driver_id]

#Danh sách WebSocket kết nối của khách hàng
connected_clients = {} 
# TASK TRACKER GHÉP ĐÔI TÀI XẾ
@app.websocket("/ws/{customer_id}/{driver_id}")
async def websocket_endpoint(websocket: WebSocket, customer_id: str, driver_id: str):
    await websocket.accept()

    # Lưu kết nối WebSocket vào danh sách theo dõi tài xế
    if driver_id not in connected_clients:
        connected_clients[driver_id] = []
    connected_clients[driver_id].append(websocket)

    #TASK TRACKER CẬP NHẬT TRẠNG THÁI NGƯỜI DÙNG
    # 1 cái là lưu để truy vấn tìm kiếm, 1 cái là để lưu cho việc tìm tài xế gần nhất
    redis_client.set(f"status_driver:{driver_id}", "busy") 
    redis_client.set(f"status_customer:{customer_id}", "have_ride") 
    db.collection("drivers").document(driver_id).update({"status":"busy"})
    db.collection("customers").document(customer_id).update({"status":"have_ride"})

    name_driver=db.collection("drivers").document(driver_id).get().to_dict().get("name")
    name_customer=db.collection("customers").document(customer_id).get().to_dict().get("name")
    request_id=redis_client.get(f"request_id:{driver_id}")
    ride_data_json = redis_client.get(f"latlonStartEnd:{request_id}")
    if ride_data_json:
        ride_data = json.loads(ride_data_json)
        lat_start = ride_data["latStart"]
        lon_start = ride_data["lonStart"]
        lat_end = ride_data["latEnd"]
        lon_end = ride_data["lonEnd"]
    db.collection("rides").document(request_id).set({
        "idCustomer":customer_id,
        "nameCustomer":name_customer,
        "idDriver":driver_id,
        "nameDriver":name_driver,
        "latStart":lat_start,
        "lonStart":lon_start,
        "latEnd":lat_end,
        "lonEnd":lon_end
    })
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
            start_time = time.time()
            data = msg.value
            driver_id = data.get("driver_id")
            # lưu thêm điếm đến và đích trong danh sách lịch sử chuyến đi
            request_id=redis_client.get(f"request_id:{driver_id}")
            redis_client.set(
            f"latlonStartEnd:{request_id}",
            json.dumps({
                "latStart": data.get("latStart"),
                "lonStart": data.get("lonStart"),
                "latEnd": data.get("latEnd"),
                "lonEnd": data.get("lonEnd")
                })
            )
            print(data)
            end_time = time.time()  # Kết thúc đo
            elapsed_time = end_time - start_time 
            print(f"Thời gian nhận yêu cầu là: {elapsed_time:.4f} giây")
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
            start_time = time.time()
            data = json.loads(msg.value.decode("utf-8"))
            driver_id = data.get("id")
            latitude = data.get("latitude")
            longitude = data.get("longitude")
            # Nếu có khách hàng đang theo dõi driver_id này, gửi WebSocket
            if driver_id in connected_clients:
                message = json.dumps({"latitude": latitude, "longitude": longitude})
                print(message)
                for ws in connected_clients[driver_id]:
                    await ws.send_text(message)
            end_time = time.time()  # Kết thúc đo
            elapsed_time = end_time - start_time 
            print(f"Thời gian nhận vị trí thời gian thực là: {elapsed_time:.4f} giây")
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
    # Cập nhật tất cả khách hàng
    customers_ref = db.collection("customers").stream()
    for customer in customers_ref:
        db.collection("customers").document(customer.id).update({"status": "no_ride"})
        redis_client.set(f"status_customer:{customer.id}", "no_ride")

    # Cập nhật tất cả tài xế
    drivers_ref = db.collection("drivers").stream()
    for driver in drivers_ref:
        db.collection("drivers").document(driver.id).update({"status": "available"})
        redis_client.set(f"status_driver:{driver.id}", "available") 
    print("✅ Đã reset trạng thái Redis khi khởi động FastAPI")
