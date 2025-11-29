CREATE TABLE district_stats (
    district_id INT PRIMARY KEY AUTO_INCREMENT,
    governorate VARCHAR(100),
    district VARCHAR(100),
    AVG_rates DECIMAL(5,2) ,
    num_pos_reviews INT DEFAULT 0,
     num_nat_reviews INT DEFAULT 0,
    num_neg_reviews INT DEFAULT 0,
    total_reviews INT DEFAULT 0 ,
    
    UNIQUE(governorate,district)
);

CREATE TABLE reviews (
    review_id INT PRIMARY KEY AUTO_INCREMENT,
    district_id INT,
    review_text TEXT,
    predicted_sentiment VARCHAR(10),
    FOREIGN KEY (district_id) REFERENCES district_stats(district_id)
);


CREATE TABLE notifications (
    district_id INT PRIMARY KEY,
    alert_message TEXT,
	num_neg_reviews INT DEFAULT 0,
    threshold INT DEFAULT 0,
    send_status VARCHAR(3) DEFAULT 'no',
    triggered_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (district_id) REFERENCES district_stats(district_id)
);



