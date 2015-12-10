How to Start the Gearpump cluster on YARN
=======================================
1. Upload the gearpump-${version}.tar.gz or gearpump-${version}.zip to remote HDFS Folder,
2. Launch the gearpump cluster on YARN
  ```bash
    yarnclient -launch /user/gearpump/gearpump.zip -storeConfig /tmp/application.conf
  ```
  If you don't specify "-launch" option, it will read default package-path from gear.conf(gearpump.yarn.client.package-path).
  Command "-storeConfig" will allow you to store the active configuration from the new started cluster.
  You can start UI server, or use shell command with this configuration file.

3. If you change the downloaded configuration file to "application.conf", and put it under class path, like conf/ folder, then
   it will be effective. To Start the UI Server, you can:
  ```bash
  ## Switch current working directory to gearpump package root directory
  copy /tmp/application.conf conf/application.conf
  bin/services
  ```