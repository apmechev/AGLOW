
import requests
import datetime

def get_singularity_image_date(container_ID='6500'):
    image_data = requests.get("https://www.singularity-hub.org/api/containers/{}/".format(container_ID)
            ).json()['files']
    created_date = image_data[0]['timeCreated']
    update_date = image_data[0]['updated']
    name = image_data[0]['name']
    created_datetime = datetime.datetime.strptime(created_date,
            "%Y-%m-%dT%H:%M:%S.%fZ")
    updated_datetime = datetime.datetime.strptime(update_date,
            "%Y-%m-%dT%H:%M:%S.%fZ")
    if created_datetime > updated_datetime:
        return created_datetime
    return updated_datetime


def get_git_repository_date(repo_user=None, repo_name=None, branch='master'):
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



