# what happens in dag
download data:
    - uses ListFiles()
    - grabs all file locations
    - checks if empty
    - deletes outputshort/outputlong
    - sorts each item to the short or long output file
    
build fact1:
    -creates outfact1 in staging

DimIP:
    - grabs ip's puts in DimIP.txt


