# script that downloads Amazon data into amazon_data subdirectory of home directory
mkdir amazon_data
wget http://snap.stanford.edu/data/amazon/productGraph/categoryFiles/meta_Grocery_and_Gourmet_Food.json.gz -P $HOME/amazon_data
wget http://snap.stanford.edu/data/amazon/productGraph/categoryFiles/reviews_Grocery_and_Gourmet_Food.json.gz -P $HOME/amazon_data
gunzip $HOME/amazon_data/meta_Grocery_and_Gourmet_Food.json.gz  
gunzip $HOME/amazon_data/reviews_Grocery_and_Gourmet_Food.json.gz
