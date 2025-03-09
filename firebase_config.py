import firebase_admin
from firebase_admin import credentials, auth, firestore

# Load tệp JSON credentials (tải từ Firebase Console)
cred = credentials.Certificate("taxidistribute-firebase-adminsdk-fbsvc-2c13d55e5e.json")
firebase_admin.initialize_app(cred)

# Kết nối Firestore để lưu user
db = firestore.client()
