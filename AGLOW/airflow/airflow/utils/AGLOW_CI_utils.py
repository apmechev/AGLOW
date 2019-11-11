import warnings
import requests
import datetime
import pickle
from AGLOW.airflow.utils.AGLOW_utils import get_task_instance

def get_all_singularity_images(collection='1999',tag=None):
    collection = requests.get("https://www.singularity-hub.org/api/collections/{}"
            .format(collection)).json()
    if tag:
        images = [c for c in collection['containers'] if c['tag']==tag]
    else:
        images = collection['containers']
    return images

def get_latest_image(images):
    for image in images:
        image['ID'] = image['detail'].split('containers/')[1]
        image['date'] = get_singularity_image_date(image['ID'])
    latest_image = images[0]
    for image in images:
        if image['date']>latest_image['date']:
            latest_image = image
    return latest_image

def get_singularity_image_date(container_ID='6500'):
    """ Gets the latest date for a singularity container hosted on 
    www.singularity-hub.org using their API. The containerID has to be 
    determined through the website. 
    """
    image_files = requests.get("https://www.singularity-hub.org/api/containers/{}/"
            .format(container_ID)).json()['files']
    if len(image_files)==0:
        warnings.warn("WARNING: No image files exist, image probably building")
        return datetime.datetime(1970,1,1) 
    created_date = image_files[0]['timeCreated']
    update_date = image_files[0]['updated']
    name = image_files[0]['name']
    created_datetime = datetime.datetime.strptime(created_date,
            "%Y-%m-%dT%H:%M:%S.%fZ")
    updated_datetime = datetime.datetime.strptime(update_date,
            "%Y-%m-%dT%H:%M:%S.%fZ")
    if created_datetime > updated_datetime:
        return created_datetime
    return updated_datetime


def get_git_repository_date(repo_user=None, repo_name=None, branch='master'):
    """Uses the GitHub API to get the date of the latest commit for a repo for
    a specific user. If no branch is selected, it defaults with 'master' branch

    """
    r=requests.get("https://api.github.com/repos/{}/{}".format(repo_user,repo_name))
    repo = r.json()
    branches = requests.get("https://api.github.com/repos/{}/{}/branches".format(repo_user,repo_name)).json()
    if branch in [i['name'] for i in branches]:
        branch_result = requests.get("https://api.github.com/repos/{}/{}/branches/{}"
                .format(repo_user,repo_name, branch)).json()
    else:
        raise RuntimeError("Branch not found")
    last_commit_date = branch_result['commit']['commit']['committer']['date']
    commit_datetime = datetime.datetime.strptime(last_commit_date,
            "%Y-%m-%dT%H:%M:%SZ")
    return commit_datetime


def check_stored_date(savefile='/home/apmechev/.prefactor_v3.0_CI.pkl', key='lofar.simg'):
    data = pickle.load(open(savefile,'rb'))
    return data[key]

def check_CI_run(ci_keys={'lofar.simg':{'type':'shub_image',
                                        'collection':'1999',
                                        'tag':'lofar'}, 
                          'prefactor/version3.0':{'type':'github_repo',
                                                  'repo_user':'lofar-astron',
                                                  'repo_name':'prefactor',
                                                  'branch':'master'}},
                savefile='/home/apmechev/.prefactor_v3.0_CI.pkl'):
    """Uses the get_commit_date method to update the date of the latest commit
       for each CI_item in the dictionary"""
    saved_runs = pickle.load(open(savefile,'rb'))
    for repo in ci_keys.keys():
        ci_keys[repo]['saved_date'] = saved_runs[repo]['saved_date']
        ci_keys[repo] = get_commit_date(ci_keys[repo])
    return ci_keys
        

def get_commit_date(dictionary):
    """Gets the most recent commit date for the CI 'item'. Currently we support
    Either shub images or github repos. The input is a CI 'item' encoded in a dictionary. 
    The function adds a 'commit_date' field to this dict and returns it. """
    if dictionary['type'] == 'shub_image':
        images = get_all_singularity_images(dictionary['collection'], tag=dictionary['tag'])
        latest_image = get_latest_image(images)
        dictionary['commit_date'] = get_singularity_image_date(container_ID=latest_image['ID'])
        dictionary['uri'] = latest_image['uri']
    if dictionary['type'] == 'github_repo':
        dictionary['commit_date'] = get_git_repository_date(repo_user=dictionary['repo_user'],
                                                             repo_name=dictionary['repo_name'],
                                                             branch=dictionary['branch'])
    return dictionary

def save_dates(save_data, savefile='/home/apmechev/.prefactor_v3.0_CI.pkl'):
    old_data = pickle.load(open(savefile,'rb'))
    for key in old_data:
        if old_data[key]['saved_date'] <= old_data[key]['commit_date']:
            save_data[key] = old_data[key]
            save_data[key]['saved_date']=datetime.datetime.now()
    pickle.dump(save_data,open(savefile,'wb'))

def _merge_two_dicts(x, y):
    z = x.copy()   # start with x's keys and values
    z.update(y)    # modifies z with y's keys and values & returns None
    return z

def save_dates_from_task(task_name, savefile='/home/apmechev/.prefactor_v3.0_CI.pkl', **context):
    task_data = get_task_instance(context, task_name)
    prev_data = pickle.load(open(savefile,'rb'))
    merged_data = _merge_two_dicts(prev_data,task_data) 
    for key in task_data.keys():
        merged_data[key]['saved_date'] = datetime.datetime.now()
    save_dates(merged_data, savefile)
    
