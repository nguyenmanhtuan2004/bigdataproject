# Online Taxi Driver Distribution System

## Description
This project uses Firebase Admin SDK for authentication and user data storage, with security configuration through `.env` file.

## Installation

### 1. Create `.env` file
Create a `.env` file in the `15_HeThongPhanPhoiLaiXeTaxiTrucTuyen_api/` directory with the following content (replace with your actual values):

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
**Note:** PRIVATE_KEY must maintain its original format, with line breaks replaced by `\\n`.

### 2. Install Python Libraries

Run the following command in the `15_HeThongPhanPhoiLaiXeTaxiTrucTuyen_api` directory:

```bash
pip install -r requirements.txt
```

### 3. Run the Application

#### Step 1: Start Kafka
```bash
cd docker-kafka-setup
docker compose up -d
```

#### Step 2: Start Redis
```bash
cd docker-redis-setup
docker compose up -d
```

#### Step 3: Start API Server
```bash
cd 15_HeThongPhanPhoiLaiXeTaxiTrucTuyen_api
python -m uvicorn main:app --reload --host 127.0.0.1 --port 8000
```

> **Note:** Ensure Docker Desktop is running before executing the above steps.

#### Application Testing
- API Documentation: http://127.0.0.1:8000/docs
- API Endpoint: http://127.0.0.1:8000/

## Firebase Configuration

- Configuration information is obtained from the Firebase Console service account JSON file, converted to `.env` format as instructed above.

