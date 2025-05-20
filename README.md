1. Push other source into Kafka
2. read data from kafka (raw)
3. push raw data into Minio
4. read data from Minio to Scaling and clean
5. Push into Minio (Scaler, csv) (Staging-area)
6. Read csv from Staging(Minio)
7. Move to Mysql(data_warehouse)
8. Create data mart based on Data_Warehouse
   
