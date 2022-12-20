# -*- coding: utf-8 -*-
import os
import datetime
from BD_json import *
from BD_server import *

sf = ServerFile("VPN")
########################################################################################
#                                BACKEND FUNCTIONS                                     #
########################################################################################

def getRoutes():

  script_path = os.path.abspath(os.path.dirname(__file__))
  json_path = os.path.join(script_path,"functions.json").replace("\\","/")
  if not os.path.exists(json_path):
      return {}

  with open(json_path,"r") as json_file:    
      content = json_file.read()

  return json.loads(content)

def getRoute(address):

  routes = getRoutes()
  return routes[address][0] if address in routes else None

#essa função deve ser usada quando vc quer que a maquina não envie resposta para um comando conhecido
def doNothing(args):

  return ""

def get_queue_files(queue = os.getenv("QUEUE_PATH")):

  files = [file for file in sf.list_dir(queue) if sf.get_path(file).endswith(".json")]
  return files

def date_diff(date_1,date_2):

  date_format_str = '%d/%m/%Y, %H:%M:%S'
  start = datetime.datetime.strptime(date_1, date_format_str)
  end = datetime.datetime.strptime(date_2, date_format_str)

  return end - start

#Calculate the difference in time between start and now.
def getRenderTime(scene_file):
  
  renderTime = None
  if "started_at" in scene_file.keys():

    now = time.strftime("%d/%m/%Y, %H:%M:%S", time.localtime())
    renderTime = date_diff(scene_file["started_at"],now)

  return renderTime

#def getFileContent(oc_file):

#  return sf.getFileContent(oc_file)

def get_current_render(files):

  return filter(rendering_files,files)

def rendering_files(file):

  file_content = sf.getFileContent(file)
  return file_content["status"] == "rendering"

def getQueueStatus(args,queue = os.getenv("QUEUE_PATH")):

  queue = queue if not queue.endswith(("\\","/")) else queue[:-1]
  queue_files = get_queue_files(queue=queue)
  current_render = get_current_render(queue_files)

  output = "NOME DA FILA: {0}\n".format(os.path.basename(queue))
  output += "RENDERIZANDO: {0}\n".format(current_render[0].path if len(current_render) > 0 else "Nada no momento")
  if len(current_render) > 0:
    render_time = getRenderTime(sf.getFileContent(current_render[0]))
    output += "TEMPO DE RENDER: {0}\n".format(render_time)
  output += "TAMANHO DA FILA: {0}\n".format(len(queue_files))
  return unicode(output,"utf-8")

def get_status_priority(file):

  default_values = {"rendered":0,"rendering":1,"waiting":2}
  c = sf.getFileContent(file)
  return default_values[c["status"]] if c["status"] in default_values.keys() else len(default_values)

def getQueuePriority(queue_config = os.getenv("QUEUE_CONFIG")):

  priority = None
  if queue_config is not None and os.path.exists(queue_config):
    config = read_json_file(queue_config)
    priority = config["Harmony"]["priority"]

  return priority

def get_renders(keyword,queue=os.getenv("QUEUE_PATH")):
    
  files = []
  name = keyword
  
  #queue = os.getenv("QUEUE_PATH")
  #files = [file for file in oc.list(queue) if file.path.endswith(".json")]

  files = get_queue_files(queue=queue)
  priority = getQueuePriority()
  if priority is not None:
    files.sort(key=lambda x:(get_status_priority(x),(0 if (priority in os.path.basename(sf.get_path(x))) else 1),sf.getLastModified(x)))
  else:
    files.sort(key=lambda x:sf.getLastModified(x))

  if len(keyword) > 0:
    files = [(files[i],i) for i in range(len(files)) if keyword in sf.get_name(files[i])]

  return files

def checkRenderStatus(args,queue = os.getenv("QUEUE_PATH")):

  keyword = ""
  if len(args) > 0:
    keyword= args[0]
  
  scenes = get_renders(keyword,queue = queue)
  queue_name = os.path.basename(queue if not queue.endswith(("\\","/")) else queue[:-1])
  msg = "FILA: {0}\n".format(queue_name)
  error_msg = "Nenhuma cena foi encontrada!"
  for scene,index in scenes:

    error_msg = ""
    scene_name = sf.get_name(scene).replace(".json","")
    scene_file = sf.getFileContent(scene)
    msg+="-------------------------------\nCENA: {0}\nPOSICAO NA FILA: {1}\nSTATUS:{2}\nTentativas: {3}\n"
    if scene_file["status"] == "rendering":
      renderTime = getRenderTime(scene_file)
      msg+="Renderizando por {}\n".format(renderTime)
    tries = len(scene_file["_render_tries"]) if "_render_tries" in scene_file.keys() else 0
    msg = msg.format(scene_name,index+1,scene_file["status"],tries)

  msg += error_msg
  output = msg if len(msg) > 0 else "Nenhum render foi encontrado na fila {0}".format(queue_name)
  
  return unicode(output,"utf-8")

def setQueuePriority(priority,queue_config = os.getenv("QUEUE_CONFIG")):

  status = "ERROR: Failed to chande render priority."
  print queue_config
  if queue_config is not None and os.path.exists(queue_config):
    data = read_json_file(queue_config)
    data["Harmony"] = {"priority": priority}
    success = write_json_file(queue_config, data)
    if success:
      status = "Priority changed!"      

  return status

def changeRenderPriority(args,queue_config = os.getenv("QUEUE_CONFIG")):

  keyword = ""
  if len(args) > 0:
    keyword= args[0]

  output = setQueuePriority(keyword,queue_config = queue_config)

  return unicode(output,"utf-8")

def getCommands(args):

  routes = getRoutes()
  output = "Comandos disponiveis:\n"
  for route in routes:
    output+= route + ": " + routes[route][-1] + "\n"

  return output

########################################################################################