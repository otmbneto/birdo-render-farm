# -*- coding: utf-8 -*-
"""
 #############################################################################
#                                                                  _    _     #
#  BD_render_little_farm.py                                       , `._) '>   #
#                                                                 '//,,,  |   #
#                                                                     )_/     #
#    by: ~camelo                   '||                     ||`       /_|      #
#    e-mail: oi@camelo.de           ||      ''             ||                 #
#                                   ||''|,  ||  '||''| .|''||  .|''|,         #
#    created: 15/11/2021            ||  ||  ||   ||    ||  ||  ||  ||         #
#    modified: 06/06/2022          .||..|' .||. .||.   `|..||. `|..|'         #
#                                                                             #
 #############################################################################

	Agenda em loop uma chamada a funcao check_for_renders() a cada 1 minuto a
	fim de checar se ha novas cenas publicadas para serem renderizadas. Quando
	houver, renderiza a cena localmente, prepara e sobe o export para o servidor.

"""

import os
import shutil
import re
import sys
import json
import time
import datetime
import ast
from oscpy.server import OSCThreadServer
import traceback

# PIP!
from dotenv import load_dotenv
from BD_utils import *

loop = "loop" in sys.argv
sf = ServerFile("VPN")

if(loop):
	import schedule

load_dotenv()

def validate_file(file_name):
	return file_name.endswith(".json") and os.path.isfile(os.path.join(os.getenv("QUEUE_PATH"), file_name))

def check_newer(file_name):
	act_tasks_names = os.listdir(os.getenv("QUEUE_PATH"))
	act_tasks_names = filter(lambda x: not x == file_name, act_tasks_names)
	act_tasks_names = filter(lambda x: re.search(r"(MNM_EP[0-9]{3}_SC[0-9]{4})_v[0-9]{2}.json$",x), act_tasks_names)
	act_tasks_names = map(lambda x : re.search(r"(MNM_EP[0-9]{3}_SC[0-9]{4})_v[0-9]{2}.json$",x).group(1), act_tasks_names)
	running_name = re.search(r"(AST_EP[0-9]{3}_SC[0-9]{4})_v[0-9]{2}.json$",file_name).group(1)
	for i in act_tasks_names :
		if i == running_name :
			print("Skipping '{}' because there is a newer version of it. Moving to '/logs'.".format(file_name))
			return False
	return True

def filter_xstage(scene_file):
	return scene_file.endswith(".xstage")

def getOutputList(d):

  output_list = []
  if "folder" in d.keys() and "file_list" in d.keys():
    output = d["folder"]
    for fl in d["file_list"]:
      if fl["render_type"] == "movie":
        output_list.append(os.path.join(output, fl["file_name"] + "." + fl["format"]).replace("\\","/"))
      else:
        output_list += ["{0}{1}{2:05d}.{3}".format(output, fl["file_name"], i, fl["format"]) for i in range(1, d["frames_number"]+1)]

  return output_list

# TEMP! ignore renders of COMP type
def filter_render_type(queue_file):

  file_d = read_json_file(queue_file)
  render_types = os.getenv("RENDER_TYPE").split(',') if os.getenv("RENDER_TYPE") is not None else []
  right_render_type = file_d["render_type"] in render_types if len(render_types) > 0 else True 
  if not right_render_type :
    print("Ignoring {}.".format(file_d["scene_path"].split('/')[-2]))
    queue_path = os.path.dirname(queue_file)
    dst = os.path.join(os.path.dirname(queue_path),os.path.basename(queue_file)).replace("\\","/")
    shutil.move(queue_file,dst)
  return right_render_type

def match_version(queue_obj):
	versions = os.listdir(os.path.join(os.getenv("PROJ_HOME"), queue_obj["scene_path"]))
	if(len(versions) == 0):
		print("No scene files in specified scene folder '{}'".format(queue_obj.scene_path))
		return False
	for scene in versions:
		if(queue_obj["version"] in scene): # FIXME [regex for safer match?]
			return scene
	return False

#TODO: Scripts should come from a dict/json.
def select_prerender_script(queue_obj):

	script_path = os.path.join(os.getenv("APPDATA"),"/Toon Boom Animation/Toon Boom Harmony Premium/2000-scripts/packages/BirdoPack/utils/")
	if queue_obj["render_type"] != "COMP" :
		script_path = os.path.join(script_path,"pre_comp_render.js")
	else:
		script_path = os.path.join(script_path,"comp_render.js")

	return script_path

def clean_folder(folder):

	itens = [os.path.join(folder,item).replace("\\","/") for item in os.listdir(folder)]
	folder_cleaned = True
	for item in itens:
		
		if(os.path.isdir(item)):
			folder_cleaned = folder_cleaned and clean_folder(item)
			if folder_cleaned:
				os.rmdir(item)
		else:
			folder_cleaned = folder_cleaned and remove_file(item)

	return folder_cleaned

def create_log(queue_obj):
	d = {}
	d["animator"] = queue_obj["animator"]
	d["episode"] = queue_obj["episode"]
	d["project"] = queue_obj["project"]
	d["rendered"] = time.strftime("%d/%m/%Y, %H:%M:%S", time.localtime())
	d["source"] = queue_obj["scene_path"]
	d["destiny"] = queue_obj["render_path"]
	d["scene"] = queue_obj["scene"]
	d["step"] = queue_obj["step"]
	d["version"] = queue_obj["version"]
	d["render_log"] = queue_obj["render_log"]
	return json.dumps(d, indent=4, sort_keys=True)

def run7Zip(zip_file, extract_to):
	exe = os.getenv("7Z")
	cmd = "\"{0}\" x {1} -o{2} -y".format(exe,zip_file,extract_to)
	print(cmd)
	return os.system(cmd)

def runHarmony(scene, preRenderScript):
	exe = os.getenv("HARMONY")
	cmd = "{0} -batch -scene \"{1}\" -preRenderScript \"{2}\"".format(exe,scene,preRenderScript)
	print("\n" + cmd + "\n")
	return os.system(cmd)

def compressScene(input_scene, output_scene):
	exe = os.getenv("FFMPEG")
	cmd = "{0} -i \"{1}\" -vcodec libx264 -pix_fmt yuv420p -g 30 -vprofile high -bf 0 -crf 23 -strict experimental -acodec aac -ab 160k -ac 2 -f mp4 \"{2}\"".format(exe, input_scene, output_scene)
	print(cmd)
	return os.system(cmd)

def save_log(queue_obj, log_path):
	log_file = log_path + "/{}__{}_EP{}_SC{}_{}.json".format(time.strftime("%Y%m%d_%Hh%Mm%Ss", time.localtime()),queue_obj["project"], queue_obj["episode"], queue_obj["scene"], queue_obj["version"])
	with open(log_file, "w") as f:
		f.write(create_log(queue_obj))
	return log_file

# upload at step 8
def try_upload(file, dst):
	print('\n\t8) Uploading the compressed version of the scene to the path specified on the json queue file...')
	success = True
	try :
		shutil.copy(file, dst)
	except :
		success = False
	if(not success):
		print("\tERROR: something went wrong while uploading the scene!")
		log = "[ERROR]: something went wrong while uploading the scene encoded export to the server!"
	else:
		log = "[OK]: everything was all right uploading the encoded export of the scene to server."
	return log

# clean at step 9
def try_cleaning(local_temp, t = "first", step = "before", suffix = "", prefix = "1)"):
	
	print("\t{0} Cleaning the local temporary folder{1}...".format(prefix, suffix))
	if(not clean_folder(local_temp)):
		log = "[WARNING]: something went wrong while cleaning the local temporary folder on the {0} time ({1} the render tasks)!".format(t, step)
	else:
		log = "[OK]: the local temporary folder was successfully cleaned (again)."
	print("\t" + log)
	return log

def start_render_log():
	render_log = {}
	steps = ["step_01_cleaning","step_02_match_version", "step_03_downloading_scene","step_04_extracting", "step_05_finding_xstage", "step_06_rendering", "step_07_reencoding", "step_08_uploading", "step_09_cleaning","step_10_writing_log"]
	for s in steps:
		render_log[s] = "Step not executed."
	return render_log


def getPSD(psd_file):

	data = read_json_file(psd_file)
	return [data["psd_file"]] if "psd_file" in data.keys() else []

def getOutputBG(frames_folder):

	outputBG = []
	export_data_folder = os.path.join(frames_folder,"EXPORT_DATA").replace("\\","/")
	if os.path.exists(export_data_folder):

		files = [os.path.join(export_data_folder,f).replace("\\","/") for f in os.listdir(export_data_folder) if f.endswith(".json")]
		for file in files:

			outputBG.append(file)
			if not file.endswith("_camera.json"):
				outputBG += getPSD(file)

	return outputBG

def remove_file(file):

	file_removed = True
	try:
		if os.path.exists(file) and os.path.isfile(file):
			os.remove(file)
	except:
		file_removed = False

	return file_removed

def create_folder(folder_path):
	
	folder_created = True
	try:
		if not os.path.exists(folder_path):
			os.makedirs(folder_path)
	except:
		folder_created = False

	return folder_created

def copy_file_to(file,to,as_=None):

	dst = os.path.join(to,as_ if as_ is not None else os.path.basename(file))
	print "Copying {0} to {1}".format(file,dst)
	if remove_file(dst):
		shutil.copyfile(file,dst)
	else:
		raise RemoveFileError

def try_send_to_vpn(files,vpn_path,clean_dst=False):

	try:
		sendToVPN(files,vpn_path,clean_dst=clean_dst)
		log = "[OK]: Files copied sucessfully!"
	except CreateServerFolderError:
		log = "[ERROR]: Unable to create VPN folder!"
	except CleanFolderError:
		log = "[ERROR]: Something went wrong trying to clean the server output folder."
	except FolderNotFoundError:
		log = "[ERROR]: VPN path not found! check if vpn is connected or if server is on!"
	except RemoveFileError:
		log = "[ERROR]: Failed to remove old version from server before copying new one."
	except:
		log = "[ERROR]: Something went wrong copying the files to vpn."

	return log

def sendToVPN(files,vpn_path,clean_dst=False):

	if not create_folder(vpn_path):
		raise CreateServerFolderError

	if clean_dst:

		if not clean_folder(vpn_path):
			raise CleanFolderError

	if os.path.exists(vpn_path):
		for f in files:
			copy_file_to(f,vpn_path)
	else:
		raise FolderNotFoundError
	
	return

def render_tasks(queue_obj):

	follow = True
	render_log = start_render_log()
	local_temp = os.getenv("LOCAL_TEMP")
	print("\n- Initializing render tasks for {}, ep{}, sc{}...\n".format(queue_obj["project"], queue_obj["episode"], queue_obj["scene"]))

	# first clean
	render_log["step_01_cleaning"] = try_cleaning(local_temp)
	follow = len(os.listdir(local_temp)) == 0
	if(not follow):
		print("\t[WARNING]: something went wrong while cleaning the local temporary folder on the first time (before downloading the scene)!")

	# find version
	print("\t2) Finding the correspondent version of the scene...")
	version_file = match_version(queue_obj)
	if(not version_file):
		print("\tERROR: version specified in the queue .json file is not on respective 'publish' folder!")
		render_log["step_02_match_version"] = "[ERROR]: version specified in the queue .json file was not on respective 'publish' folder!"
		follow = False
	else:
		render_log["step_02_match_version"] = "[OK]: the version specified in the queue .json had a match on the 'publish' folder."

	# download
	filename = os.path.join(os.getenv("PROJ_HOME"), queue_obj["scene_path"], version_file)
	if(follow):
		print("\t3) Downloading the zipped scene file...")
		try :
			shutil.copy(filename, local_temp)
		except :
			follow = False

	if(not follow):
		print("\tERROR: something went wrong while downloading the zipped scene file!")
		render_log["step_03_downloading_scene"] = "[ERROR]: something went wrong while downloading the zipped scene file!"
	else:
		render_log["step_03_downloading_scene"] = "[OK]: everything was fine while downloading the zipped scene file!"

	# extract
	if(follow):
		print("\t4) Extracting scene...")
		zip_file = os.path.join(local_temp, version_file).replace("\\","/")
		follow = (run7Zip(zip_file, local_temp) == 0)

	if(not follow):
		print("\n\tERROR: something went wrong while extracting scene from the zipped file!")
		render_log["step_04_extracting"] = "[ERROR]: something went wrong while extracting the scene from the zipped file!"
	else:
		render_log["step_04_extracting"] = "[OK]: everything was fine while extracting the zipped scene file!"
	print("")

	# find more recent .xstage
	newest_xstage = ""
	if(follow):
		print("\t5) Searching for the newest *.xstage file of the scene...")
		scene_files = os.listdir(local_temp + "/" + "{}_EP{}_SC{}".format(queue_obj["project"], queue_obj["episode"], queue_obj["scene"]))
		scene_files = filter(filter_xstage, scene_files)
		scene_files.sort()
		try:
			newest_xstage = scene_files[-1]
		except:
			print("ERROR: apparently there is no valid '*.xstage' file inside the scene folder!")
			render_log["step_05_finding_xstage"] = "[ERROR]: apparently there is no valid '*.xstage' file inside the scene folder!"
			follow = False
		else:
			print("\t\t(apparently is the '{}').".format(newest_xstage))
			render_log["step_05_finding_xstage"] = "[OK]: found the newest '*.xstage' (was the '{}')!".format(newest_xstage)

	# render!
	if(follow):
		print('\t6) Rendering the "Write_FINAL" node of the scene...\n')
		shot = "{0}_EP{1}_SC{2}".format(queue_obj["project"], queue_obj["episode"], queue_obj["scene"])
		scene = os.path.join(local_temp, shot, newest_xstage).replace("\\","/")
		harmony_return = runHarmony(scene, select_prerender_script(queue_obj))
		follow = harmony_return == 0 or harmony_return == 100 or harmony_return == 12

		if(not follow):
			print("\tERROR: something went wrong while rendering the scene!")
			render_log["step_06_rendering"] = "[ERROR]: something went wrong while rendering the scene (if recurrent, check Harmony License)"
		else:
			render_log["step_06_rendering"] = "[OK]: the scene was sucessfully rendered by the render-little-farm."
  
  # compress
	if queue_obj["render_type"] != "COMP" : 
		if(follow):
			print('\n\t7) Reencoding the rendered scene...\n')
			input_scene = os.path.join(local_temp,'{0}_EP{1}_SC{2}/'.format(queue_obj["project"], queue_obj["episode"], queue_obj["scene"]), "frames/exportFINAL.mov").replace("\\","/")
			compressed = os.path.join(os.path.dirname(input_scene),"{}_EP{}_SC{}.mov".format(queue_obj["project"], queue_obj["episode"], queue_obj["scene"])).replace("\\","/")
			follow = (compressScene(input_scene, compressed) == 0)

			if(not follow):
				print("\tERROR: something went wrong while compressing the scene!")
				render_log["step_07_reencoding"] = "[ERROR]: something went wrong while reencoding the scene!"
			else:
				#faster this way :(
				render_log["step_07_reencoding"] = "[OK]: the scene export was properly encoded."
				render_log["step_08_uploading"] = try_upload(compressed, queue_obj["render_path"])
	else:
		render_log["step_07_reencoding"] = "[OK]: comp render, bypassing the compress phase."
		scene_folder = os.path.dirname(scene)
		render_data = os.path.join(scene_folder,"_renderData.json").replace("\\","/")
		if os.path.exists(render_data):
			output_content = read_json_file(render_data)
			server_path = output_content["render_comp"]
			files = getOutputList(output_content) #pega o caminho dos outputs
			render_log["step_08_uploading"] = try_send_to_vpn(files,server_path)
			#pegas os psdss do BG
			server_path = os.path.dirname(server_path)
			server_path = os.path.join(os.path.dirname(server_path),"03_BG")
			files = getOutputBG(output_content["folder"] if "folder" in output_content.keys() else "")
			render_log["step_08_uploading"] = try_send_to_vpn(files,server_path)
		else:
			render_log["step_08_uploading"] = "ERROR: _renderData.json not found!"

	render_log["step_09_cleaning"] = try_cleaning(local_temp, t = "second", step = "after", suffix = " (again)", prefix = "9)")

	# write log
	print("\t10) Writing a local render log...")
	render_log["step_10_writing_log"] = "[OK]: (if ou are reading this) the log for this renderer was written correctly!"
	queue_obj["render_log"] = render_log
	save_log(queue_obj, os.getenv("LOG_PATH"))
	print("\n- End of render tasks for {}_EP{}_SC{}_{}, ;) !".format(queue_obj["project"], queue_obj["episode"], queue_obj["scene"], queue_obj["version"]))
	return [follow, render_log]

def update_render_file(file_path, data):
	task_file = open(file_path, 'r')
	task_content = task_file.read()
	task_file.close()
	json_data = json.loads(task_content)
	for k in data:
		if not k in json_data.keys():
			json_data[k] = data[k]
		elif(type(json_data[k]) is list):
			if(type(data[k]) is list):
				json_data[k] += data[k]
			else:
				json_data[k].append(data[k])
		else:
			json_data[k] = data[k]
	task_file = open(file_path, 'w')
	task_file.write(json.dumps(json_data, indent = 4, sort_keys = True))
	task_file.close()
	return json_data

def render_file(file_name):
	file_path = os.path.join(os.getenv("QUEUE_PATH"), file_name)
	render_data = update_render_file(file_path, {"started_at": time.strftime("%d/%m/%Y, %H:%M:%S", time.localtime()),"status": "rendering"})
	render_status = render_tasks(render_data)
	r_status = "rendered" if render_status[0] else "not rendered"
	entry = {"started_at": render_data["started_at"], "_status": r_status,"render_log": render_status[1],"__timestamp":time.strftime("%d/%m/%Y, %H:%M:%S", time.localtime())}
	data = {"status":r_status, "_render_tries": [entry]}
	update_render_file(file_path, data)
	print "The render was fine! Moving the task file to the 'logs' folder." if render_status[0] else "Something happens during the render. I'll keep the task file and try again soon."
	return render_status[0]

def check_for_renders():
	queue_path = os.getenv("QUEUE_PATH")
	queue = filter(validate_file, os.listdir(queue_path))
	queue = filter(filter_render_type,[os.path.join(queue_path,f) for f in queue])
	queue = [os.path.basename(x) for x in queue]#gambiarra(TODO: mudar isso)

	queue.sort(key = lambda x: os.path.getmtime(queue_path + x))
	if (len(queue) == 0):
		print("No new itens to render now ({}) =] .".format(time.strftime("%d/%m/%Y, %H:%M:%S", time.localtime())))
		return
	for file_name in queue :
		if(not check_newer(file_name)):
			# update and move 'task-file-log' and writing local log
			task_file_full_path = os.path.join(os.getenv("QUEUE_PATH"))
			save_log(task_data, os.getenv("LOG_PATH"))
			task_data = update_render_file(task_file_full_path, {"_render_tries":"Skipping '{}' because there is a newer version of it. Moving to '/logs'.".format(file_name)})
			dst = os.path.join(queue_path, "_logs\\" + time.strftime("%Y%m%d_%Hh%Mm%Ss__", time.localtime()) + file_name)
			shutil.move(task_file_full_path, dst)
			return
		if(render_file(file_name)):
			src = os.path.join(queue_path, file_name)
			dst = os.path.join(queue_path, "_logs\\" + time.strftime("%Y%m%d_%Hh%Mm%Ss__", time.localtime()) + file_name)
			shutil.move(src, dst)
	return

def parseCommand(command,args):

  output = None
  f = getRoute(command)
  if f is not None:
    output = eval(f + "("+ str(args) +")")

  return output

def answer(response,port = None,route=b'/response',encode="utf-8"):

  print(response)
  response = str(response).encode(encode)
  print(response)
  osc.answer(route,[response],port=port)

  return

def heartbeatReceived(*msg):

  try:
    request = ast.literal_eval(msg[0].decode("utf-8"))
    print(request)
    answer(request,port=int(request["port"]),route=b'/heartbeat')
  except:
    traceback.print_exc()
  return 

def messageReceived(*msg):

  try:

    request = ast.literal_eval(msg[0].decode("utf-8"))
    cmd = request["command"]
    args = request["args"]
    output = parseCommand(cmd,args)
    if output is None:
      output = "ERRO: Comando desconhecido: {0}".format(msg[0])

    #se essa e a primeira mensagem,comecar com hello user e abrir aspas triplas.
    if request["sequence_number"] == 0:
      header = 'Hello <@{0}>\n```'.format(request["user_id"])
      output = header + output

    #se essa e a ultima mensagem,fechar aspas triplas
    if request["size"] == request["sequence_number"] + 1:
      output += "```"

    response = {"reply_to": request["reply_to"],"sequence_number":request["sequence_number"],"response": output}
    print "Generating output"
    print output

    answer(response,port=int(request["port"]),encode="latin1")
  except:
    traceback.print_exc()
  return 

def createServer():

  osc = OSCThreadServer()  # See sources for all the arguments
  # You can also use an \*nix socket path here

  SERVER_IP = os.getenv("SERVER_IP")
  SERVER_PORT = int(os.getenv("SERVER_PORT"))
  if SERVER_PORT is not None and SERVER_IP is not None:
    sock = osc.listen(address=SERVER_IP, port=SERVER_PORT, default=True)
    osc.bind(b'/request',messageReceived)
    osc.bind(b'/heartbeat',heartbeatReceived)

  return osc

if __name__ == '__main__' :

	print("Creating server\n\n")
	osc = createServer() #servidor que recebe msgs do discord bot

	if(loop):
		check_for_renders()
		schedule.every(1).minutes.do(check_for_renders)
		while True:
			schedule.run_pending()
			time.sleep(10)
	else:
		check_for_renders()
