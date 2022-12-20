import os
import owncloud
import shutil
import json
import datetime

class ServerFile():

	def __init__(self,server_type,root="",site=os.getenv("OC_CLIENT"),user=os.getenv("OC_LOGIN"),password=os.getenv("OC_PASSWD"),CHECK_CERTS=True):

		self.SERVER_TYPE = server_type
		self.ROOT = root
		self.site = site
		self.user = user
		self.password = password
		self.oc = None
		self.check_certs = CHECK_CERTS

		if self.SERVER_TYPE != "VPN":
			self.oc = login(self.site,self.user,self.password,CHECK_CERTS=self.check_certs)

	def login(site,user,password,CHECK_CERTS=True):

	    owncloud_client = owncloud.Client(site,verify_certs=CHECK_CERTS)
	    owncloud_client.login(user,password)

	    return owncloud_client

	def list_dir(self,p):

	    files = None
	    if self.SERVER_TYPE == "NEXTCLOUD":
	        files = self.oc.list(p.path if not isinstance(p, str) else p)
	    else:
	        files = [os.path.join(p,f) for f in os.listdir(p)]
	    return files

	def get_name(self,p):

	    name = None
	    if self.SERVER_TYPE == "NEXTCLOUD":
	        name = p.get_name()
	    else:
	        name = os.path.basename(p)
	    return name

	def get_path(self,p):

	    path = None
	    if self.SERVER_TYPE == "NEXTCLOUD":
	        path = p.path
	    else:
	        path = p
	    return path

	def remove_file(self,p):

	    if self.SERVER_TYPE == "NEXTCLOUD":
	        self.oc.delete(p)
	    else:
	        os.remove(p)

	def getFileContent(self,oc_file):

	    if self.SERVER_TYPE == "NEXTCLOUD":
	        data = self.oc.get_file_contents(oc_file)
	    else:
	        with open(oc_file, 'r') as fp:
	            data = fp.read()
	    return json.loads(data)

	def putFileContent(self,oc_file,json_data):

	    if self.SERVER_TYPE == "NEXTCLOUD":
	        self.oc.put_file_contents(oc_file, json.dumps(json_data, indent = 4, sort_keys = True))
	    else:
	        with open(oc_file, 'w') as fp:
	            json.dump(json_data, fp, indent=4, sort_keys=True)
	    return

	def is_dir(self,p):

	    isDir = False
	    if self.SERVER_TYPE == "NEXTCLOUD":
	        isDir = p.is_dir()
	    else:
	        isDir = os.path.isdir(p)

	    return isDir

	def move_file(self,org,dst):

	    if self.SERVER_TYPE == "NEXTCLOUD":
	        self.oc.move(org,dst)
	    else:
	        shutil.copyfile(org,dst)
	        os.remove(org)
	    return

	def getLastModified(self,file):

		lastModified = None
		if self.SERVER_TYPE == "NEXTCLOUD":
			lastModified = datetime.datetime.strptime(file.attributes["{DAV:}getlastmodified"], "%a, %d %b %Y %H:%M:%S %Z").strftime("%Y%m%d_%H%M%S")
		else:
			lastModified = datetime.datetime.fromtimestamp(os.path.getmtime(file)).strftime('%Y%m%d_%H%M%S')

		return lastModified

	def path_exists(self,path):

		if self.SERVER_TYPE == "NEXTCLOUD":
			exists = self.oc.file_info(path) is not None
		else:
			exists = os.path.exists(path)

	def isdir(self,file):

		if self.SERVER_TYPE == "NEXTCLOUD":
			isFile = file.is_dir()
		else:
			isFile = os.path.isdir(file)

		return isFile

	def isfile(self,file):

		return not self.isdir(file)