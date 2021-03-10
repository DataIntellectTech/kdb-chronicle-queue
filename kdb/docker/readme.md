(1) Build image

cd ./kdb/docker

docker build -t frielmark/adapter-kdb .


(2) Run image

docker run -it --rm -p 5000:5000 -t frielmark/adapter-kdb

Should open up a q prompt.

When adapter starts, there will be a lot of "noise" appears here. Just q outputting to screen. 
When it stops you can hit Enter and get back to a prompt.

When you have a prompt you can use any of the commands below (without quote marks):

"quote"
Should list the structure of the quote table. And show any rows.

"count quote"
Should return the number of rows in the table.

"select ct:count sym by sym from quote"
Should return count grouped by sym column.

