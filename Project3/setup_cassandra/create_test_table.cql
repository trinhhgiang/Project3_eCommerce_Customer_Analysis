-- Create category namespace
CREATE KEYSPACE category WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};
CREATE KEYSPACE brand WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};


-- Create table
CREATE TABLE brand.prices_countings_brand ( start_time TIMESTAMP,   
 end_time TIMESTAMP,
 brand TEXT, 
 view_count INT, 
 cart_count INT, 
 remove_cart_count INT, 
 purchase_count INT, 
 view_revenue DOUBLE, 
 cart_revenue DOUBLE, 
 remove_cart_revenue DOUBLE, 
 purchase_revenue DOUBLE,
 average_price DOUBLE,  
 PRIMARY KEY (start_time, end_time, brand));

CREATE TABLE category.prices_countings_category ( start_time TIMESTAMP,   
 end_time TIMESTAMP,
 category text, 
 sub_category text,
 view_count INT, 
 cart_count INT, 
 remove_cart_count INT, 
 purchase_count INT, 
 view_revenue DOUBLE, 
 cart_revenue DOUBLE, 
 remove_cart_revenue DOUBLE, 
 purchase_revenue DOUBLE,
 average_price DOUBLE,  
 PRIMARY KEY (start_time, end_time, category, sub_category));


CREATE KEYSPACE ecommerce WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

CREATE TABLE ecommerce.cluster_summary (
    cluster INT PRIMARY KEY,
    user_count INT,
    avg_activity_frequency FLOAT,
    avg_unique_products FLOAT,
    avg_spending FLOAT,
    avg_unique_brands FLOAT
);

CREATE TABLE ecommerce.user_clusters (
    user_id TEXT PRIMARY KEY,
    activity_frequency INT,
    unique_products INT,
    unique_brands INT,
    average_spend FLOAT,
    cluster INT
);

CREATE TABLE ecommerce.user_clusters_pca (
    user_id TEXT PRIMARY KEY,
    pca_x FLOAT,
    pca_y FLOAT,
    cluster INT
);
