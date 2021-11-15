python -m venv E:\DTS
REM Copy all folders and application to E:\DTS\App folder
REM mkdir E:\DTS\App
robocopy E:\Users\lmartinez\Documents\GitHub\edg-dts-main\Python\DTS E:\DTS\App *.* /XF test*.py 
robocopy /S E:\Users\lmartinez\Documents\GitHub\edg-dts-main\Python\DTS\batch E:\DTS\batch *.*
robocopy /S E:\Users\lmartinez\Documents\GitHub\edg-dts-main\Python\DTS\configs E:\DTS\configs *.*
robocopy /S E:\Users\lmartinez\Documents\GitHub\edg-dts-main\Python\DTS\database E:\DTS\database *.*
robocopy /S E:\Users\lmartinez\Documents\GitHub\edg-dts-main\Python\DTS\ftp E:\DTS\ftp *.*
robocopy /S E:\Users\lmartinez\Documents\GitHub\edg-dts-main\Python\DTS\utils E:\DTS\utils *.*
robocopy /S E:\Users\lmartinez\Documents\GitHub\edg-dts-main\Python\DTS\vault E:\DTS\vault *.*
E:\DTS\scripts\activate.bat
REM pip install -r requirements.txt
REM cd E:\DTS\App
python -m pip install -r E:\DTS\App\requirements.txt
REM Create Temp Folders and add "Everyone" full access to both folders
mkdir E:\Temp
mkdir E:\services
