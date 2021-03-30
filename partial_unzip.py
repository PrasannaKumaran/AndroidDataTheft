import os 
def unzip(path, nrows, skiprows):
	"""
        Unzip large files partially
        Parameters:
            path (String) : path of the file to unzip
            nrows (int) : number of rows to extract
            skiprows (int) : number of rows to skip
		Returns:
            partially unzipped tsv
	"""
	command = 'gunzip < {} | tail -n +{} | head -n {} > {}.tsv'.format(path,skiprows + 1, nrows, path.replace('.zip', ''))
	os.system(command)
	print("Written to {}".format(path.replace('.zip','')))
	
