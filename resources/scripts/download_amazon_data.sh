# script that downloads Amazon data into amazon_data subdirectory of home directory
mkdir $APP_ROOT/amazon_data
wget http://snap.stanford.edu/data/amazon/productGraph/categoryFiles/meta_Grocery_and_Gourmet_Food.json.gz -P $APP_ROOT/amazon_data
wget http://snap.stanford.edu/data/amazon/productGraph/categoryFiles/reviews_Grocery_and_Gourmet_Food.json.gz -P $APP_ROOT/amazon_data
gunzip $APP_ROOT/amazon_data/meta_Grocery_and_Gourmet_Food.json.gz  
gunzip $APP_ROOT/amazon_data/reviews_Grocery_and_Gourmet_Food.json.gz
