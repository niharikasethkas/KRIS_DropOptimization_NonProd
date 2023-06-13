# import subprocess
# import sys

from app import config

with open(config.INSTALL_PATH + "requirements.txt", "r") as f:
    req = f.read()

packages = [i for i in req.split("\n") if len(i) > 0]


def install_packages_func():
    # for pckg in packages:
    #     subprocess.check_call([sys.executable, "-m", "pip", "install", pckg])
    print(packages)
