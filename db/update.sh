gsutil cp gs://clean_items/news/2022-07-21.tar.xz $HOME/NewsLab/db/tdata/news/
gsutil cp cp gs://clean_items/tweets/news_2022-07-21 00-00-01 $HOME/NewsLab/db/tdata/tweets/

files="$HOME/NewsLab/db/tdata/news/*"
for f in $files
do
	tar xvf "$f" -C $HOME/NewsLab/db/cdata/news
done

files="$HOME/NewsLab/db/tdata/tweets/*"
for f in $files
do
	tar xvf "$f" -C $HOME/NewsLab/db/cdata/tweets
done