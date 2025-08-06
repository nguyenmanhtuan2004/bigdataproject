# Hệ Thống Phân Phối Lái Xe Taxi Trực Tuyến

## Mô tả
Dự án sử dụng Firebase Admin SDK để xác thực và lưu trữ dữ liệu người dùng, cấu hình bảo mật qua file `.env`.

## Cài đặt

### 1. Tạo file `.env`
Tạo file `.env` trong thư mục `15_HeThongPhanPhoiLaiXeTaxiTrucTuyen_api/` với nội dung như sau (thay giá trị tương ứng):

```env
FIREBASE_TYPE=service_account
FIREBASE_PROJECT_ID=your_project_id
FIREBASE_PRIVATE_KEY_ID=your_private_key_id
FIREBASE_PRIVATE_KEY="-----BEGIN PRIVATE KEY-----\\nYOUR_PRIVATE_KEY\\n-----END PRIVATE KEY-----\\n"
FIREBASE_CLIENT_EMAIL=your_client_email@your_project.iam.gserviceaccount.com
FIREBASE_CLIENT_ID=your_client_id
FIREBASE_AUTH_URI=https://accounts.google.com/o/oauth2/auth
FIREBASE_TOKEN_URI=https://oauth2.googleapis.com/token
FIREBASE_AUTH_PROVIDER_X509_CERT_URL=https://www.googleapis.com/oauth2/v1/certs
FIREBASE_CLIENT_X509_CERT_URL=https://www.googleapis.com/robot/v1/metadata/x509/your_client_email@your_project.iam.gserviceaccount.com
FIREBASE_UNIVERSE_DOMAIN=googleapis.com
```
**Lưu ý:** PRIVATE_KEY phải giữ nguyên định dạng, các dòng xuống dòng thay bằng `\\n`.

### 2. Cài đặt thư viện Python

Chạy lệnh sau trong thư mục `15_HeThongPhanPhoiLaiXeTaxiTrucTuyen_api`:

```bash
pip install -r requirements.txt
```

### 3. Chạy ứng dụng

```bash
python main.py
```

## Cấu hình Firebase

- Thông tin cấu hình lấy từ file service account JSON của Firebase Console, chuyển sang file `.env` như hướng dẫn trên.

