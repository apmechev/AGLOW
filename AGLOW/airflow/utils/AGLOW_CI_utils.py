import requests
import datetime
import pickle

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
        print("WARNING: No image files exist, image probably building")
        return None
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
                                                  'branch':'version3.0'}},
                savefile='/home/apmechev/.prefactor_v3.0_CI.pkl'):
    saved_runs = pickle.load(open(savefile,'rb'))
    for repo in ci_keys.keys():
        ci_keys[repo]['saved_date'] = saved_runs[repo]
        ci_keys[repo] = get_date_from_dict(ci_keys[repo])
    return ci_keys
        

def get_date_from_dict(dictionary):
    if dictionary['type'] == 'shub_image':
        images = get_all_singularity_images(dictionary['collection'], tag=dictionary['tag'])
        latest_image = get_latest_image(images)
        dictionary['current_date'] = get_singularity_image_date(container_ID=latest_image['ID'])
        dictionary['uri'] = latest_image['uri']
    if dictionary['type'] == 'github_repo':
        dictionary['current_date'] = get_git_repository_date(repo_user=dictionary['repo_user'],
                                                             repo_name=dictionary['repo_name'],
                                                             branch=dictionary['branch'])
    return dictionary


