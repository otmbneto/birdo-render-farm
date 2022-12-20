import json

def write_json_file(json_file, data, sort_dic=False):
    """Saves object (dictionary) into json file"""
    with open(json_file, 'w') as fp:
        try:
            json.dump(data, fp, indent=2, sort_keys=sort_dic)
        except:
            print "error writing json file: {0}".format(json_file)
            return False
    return True

def read_json_file(file):

	content = None
	with open(file, 'r') as fp:
		content = json.loads(fp.read())

	return content

#TODO: Criar um update_json_file
