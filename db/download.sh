gsutil -m cp gs://clean_items/news/* $HOME/NewsLab/db/tdata/news/
gsutil -m cp gs://clean_items/tweets/* $HOME/NewsLab/db/tdata/tweets/

files="$HOME/NewsLab/db/tdata/news/*"
for f in $files
do
	tar xvf $f -C $HOME/NewsLab/db/cdata/news
done

files="$HOME/NewsLab/db/tdata/tweets/*"
for f in $files
do
	tar xvf $f -C $HOME/NewsLab/db/cdata/tweets
done
