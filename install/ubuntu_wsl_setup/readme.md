# Setup Ubuntu for Windows Subsystem for Linux

## Overview
We follow the recommendation of WSL, use Windows file system for working folder, and linux programs access Windows file system under */mnt* folder. In detail:
* Clone git repository on Windows
* Install python on linux
* Create python venv in Windows file system, but the files are in linux format
* Install [VcXsrv](https://sourceforge.net/projects/vcxsrv/) on Windows as X server for WSL
* Install [PyCharm Community version for Linux](https://www.jetbrains.com/pycharm/download/#section=linux) on WSL

Below are setup details steps:

## Make ubuntu login default as root
We always switch to root to run IDE and python, because files on windows file system appear to be owned by root in linux. Run command below in **Windows command prompt as administrator**, assuming you have installed Ubuntu 18.04 LTS:

    ubuntu1804 config --default-user root

Note that after this step, as long as ubuntu terminal is running, you can access your linux file system from Windows through *\\\\wsl$\\Ubuntu-18.04\\root*, provided that your Windows 10 is at least version 1903.

## Fix linux terminal settings
Note that these steps are intended to make it easy for developers to use linux terminal to find files and run python script. You could skip this step if you don't need to do so.

Run these commands in WSL:

    echo 'set background=dark' >> ~/.vimrc
    echo -E "export LS_COLORS='rs=0:di=01;93:ln=01;36:mh=00:pi=40;33:so=01;35:do=01;35:bd=40;33;01:cd=40;33;01:or=40;31;01:mi=00:su=37;41:sg=30;43:ca=30;41:tw=30;42:ow=34;42:st=37;44:ex=01;32:*.tar=01;31:*.tgz=01;31:*.arc=01;31:*.arj=01;31:*.taz=01;31:*.lha=01;31:*.lz4=01;31:*.lzh=01;31:*.lzma=01;31:*.tlz=01;31:*.txz=01;31:*.tzo=01;31:*.t7z=01;31:*.zip=01;31:*.z=01;31:*.Z=01;31:*.dz=01;31:*.gz=01;31:*.lrz=01;31:*.lz=01;31:*.lzo=01;31:*.xz=01;31:*.zst=01;31:*.tzst=01;31:*.bz2=01;31:*.bz=01;31:*.tbz=01;31:*.tbz2=01;31:*.tz=01;31:*.deb=01;31:*.rpm=01;31:*.jar=01;31:*.war=01;31:*.ear=01;31:*.sar=01;31:*.rar=01;31:*.alz=01;31:*.ace=01;31:*.zoo=01;31:*.cpio=01;31:*.7z=01;31:*.rz=01;31:*.cab=01;31:*.wim=01;31:*.swm=01;31:*.dwm=01;31:*.esd=01;31:*.jpg=01;35:*.jpeg=01;35:*.mjpg=01;35:*.mjpeg=01;35:*.gif=01;35:*.bmp=01;35:*.pbm=01;35:*.pgm=01;35:*.ppm=01;35:*.tga=01;35:*.xbm=01;35:*.xpm=01;35:*.tif=01;35:*.tiff=01;35:*.png=01;35:*.svg=01;35:*.svgz=01;35:*.mng=01;35:*.pcx=01;35:*.mov=01;35:*.mpg=01;35:*.mpeg=01;35:*.m2v=01;35:*.mkv=01;35:*.webm=01;35:*.ogm=01;35:*.mp4=01;35:*.m4v=01;35:*.mp4v=01;35:*.vob=01;35:*.qt=01;35:*.nuv=01;35:*.wmv=01;35:*.asf=01;35:*.rm=01;35:*.rmvb=01;35:*.flc=01;35:*.avi=01;35:*.fli=01;35:*.flv=01;35:*.gl=01;35:*.dl=01;35:*.xcf=01;35:*.xwd=01;35:*.yuv=01;35:*.cgm=01;35:*.emf=01;35:*.ogv=01;35:*.ogx=01;35:*.aac=00;36:*.au=00;36:*.flac=00;36:*.m4a=00;36:*.mid=00;36:*.midi=00;36:*.mka=00;36:*.mp3=00;36:*.mpc=00;36:*.ogg=00;36:*.ra=00;36:*.wav=00;36:*.oga=00;36:*.opus=00;36:*.spx=00;36:*.xspf=00;36:'" >> ~/.bashrc
    echo -E "export PS1='${debian_chroot:+($debian_chroot)}\[\033[01;32m\]\u@\h\[\033[00m\]:\[\033[01;33m\]\w\[\033[00m\]\$ '" >> ~/.bashrc
    echo -E "export PYTHONPATH=.:${PYTHONPATH}" >> ~/.bashrc

exit current Ubuntu terminal and start a new one, to make these settings effective.

## Prepare system
Run these commands in WSL:

    apt-get update -y && apt-get upgrade -y
    apt-get install -y python3 python3-venv python3-pip libgflags-dev libsnappy-dev zlib1g-dev libbz2-dev liblz4-dev libzstd-dev librocksdb-dev
    python3 -m pip install --upgrade pip

## Download code to Windows 
Assuming the working folder is *c:\work\gs-framework*, run this command in Windows shell or do the same thing with git client:

    git clone http://git.graphstrategist.com:8787/git/gft/gs-framework.git c:\work\gs-framework

## Create python venv without pip
In WSL:

    mkdir ~/.pyenv
    python3 -m venv ~/.pyenv/gs-framework --without-pip --system-site-packages
    source ~/.pyenv/gs-framework/bin/activate
    cd /mnt/c/work
    python -m pip install -e gs-framework

Note: Because of [a problem of ubuntu](https://askubuntu.com/questions/879437/ensurepip-is-disabled-in-debian-ubuntu-for-the-system-python/897004), pip cannot be included in python venv. 

## Run PyChram Professional version in Windows

You can choose to run PyChram Professional version in Windows or run PyChram Community version in WSL and using XServer. It is much simpler to run PyChram Professional version in Windows, but a license for PyChram Professional version is required.

To run PyChram Professional version in Windows, you will need to download [Professional version of PyCharm](https://www.jetbrains.com/pycharm/download/#section=windows) and get a licencse [here](http://idea.lanyus.com/).

Then in PyCharm project settings, follow [this link](https://www.jetbrains.com/help/pycharm/using-wsl-as-a-remote-interpreter.html) to configure a remote interpreter using WSL, but choose the python executable in your venv folder (for example: *~/.pyenv/gs-framework/bin/python*) instead of */usr/bin/python3*.

## Run PyChram Community version in WSL and using XServer
Note that this method doesn't need any license or crack, but is more complex.

### Install GUI packages in WSL

    apt-get install -y libxtst6 ttf-wqy-microhei vim-gtk

### Set Display to our X Server in WSL
    echo -E "export DISPLAY=:0.0" >> ~/.bashrc
exit current Ubuntu terminal and start a new one, to make the setting effective.

### Install [VcXsrv](https://sourceforge.net/projects/vcxsrv/) 

Just download and run it. All the default options are fine.

### Install [PyCharm Community version for Linux](https://www.jetbrains.com/pycharm/download/#section=linux) in WSL

Assuming the downloaded version is *pycharm-community-2019.2*. In WSL:

    cd /opt
    curl -L -O https://download.jetbrains.com/python/pycharm-community-2019.2.tar.gz
    tar -xzf pycharm-community-2019.2.tar.gz
    ln -s pycharm-community-2019.2 pycharm
    cd /usr/local/bin
    ln -s /opt/pycharm/bin/pycharm.sh pycharm
    pycharm &

Please specify python in venv created above (*~/.pyenv/gs-framework/bin/python*) as your project interpreter in PyCharm settings
