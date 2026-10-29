-- Local isolated test identity. Never mounted into development volumes.
CREATE DATABASE IF NOT EXISTS mock_platform1 CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin;
CREATE USER IF NOT EXISTS 'mock_p1'@'%' IDENTIFIED BY 'test-p1-local-only';
GRANT ALL PRIVILEGES ON mock_platform1.* TO 'mock_p1'@'%';
CREATE DATABASE IF NOT EXISTS partner_hub CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin;
CREATE USER IF NOT EXISTS 'hub'@'%' IDENTIFIED BY 'test-hub-local-only';
GRANT ALL PRIVILEGES ON partner_hub.* TO 'hub'@'%';
CREATE DATABASE IF NOT EXISTS mock_platform2 CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin;
CREATE USER IF NOT EXISTS 'mock_p2'@'%' IDENTIFIED BY 'test-p2-local-only';
GRANT ALL PRIVILEGES ON mock_platform2.* TO 'mock_p2'@'%';
