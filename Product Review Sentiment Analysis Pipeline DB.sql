
CREATE DATABASE IF NOT EXISTS product_sentiment_analysis_db;
USE product_sentiment_analysis_db;

-- 2. Create producta table
CREATE TABLE IF NOT EXISTS product (
    product_id VARCHAR(50)  PRIMARY KEY,
    product_name VARCHAR(255) NOT NULL,
    AVG_price DECIMAL(10,2) NOT NULL,
    AVG_rates DECIMAL(3,2),
    num_pos_reviews INT DEFAULT 0,
    num_nat_reviews INT DEFAULT 0,
    num_neg_reviews INT DEFAULT 0,
    total_reviews INT DEFAULT 0
);



-- 3. Create reviews table
CREATE TABLE IF NOT EXISTS reviews (
    review_id INT AUTO_INCREMENT PRIMARY KEY,
    review_text TEXT NOT NULL,
    prod_id VARCHAR(50) NOT NULL,
    review_date DATETIME DEFAULT CURRENT_TIMESTAMP,
    predicted_sentiment VARCHAR(10),
    FOREIGN KEY (prod_id) REFERENCES product(product_id) ON DELETE CASCADE
);



-- 4. Create notification table
CREATE TABLE IF NOT EXISTS notification (
    notification_id INT AUTO_INCREMENT PRIMARY KEY,
    product_id VARCHAR(50) NOT NULL,
    message TEXT NOT NULL,
    triggered_at DATETIME DEFAULT CURRENT_TIMESTAMP,
	send_status VARCHAR(3) DEFAULT 'no',
    num_negative INT DEFAULT 0,
    threshold INT DEFAULT 0,
    FOREIGN KEY (product_id) REFERENCES product(product_id) ON DELETE CASCADE
);
