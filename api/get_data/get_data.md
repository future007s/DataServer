# 使用说明

1. 安装依赖

    - 安装python3.11版本
    - 运行以下命令安装依赖
    ```bash
    pip install --no-index --ignore-installed --find-links=/packages -r requirements.txt
    ```
2. 修改配置文件

    ```txt
    参数说明：
    - user_info: 用户信息,修改为自己的用户名和密码
    - default_time：默认启动时间，第一次启动时，会从该时间开始上传数据
    - max_workers：最大进程数
    - get_url：索要数据的url地址
    - login_url：登录的url地址
    - flag：是否查询新表，0代表旧表，1代表新表。
    - database_info：数据存储的数据库信息
    - data_transfer_time：保存最近一次读取的时间的表名
    - building_info_list：建筑信息
      - building_id:建筑id
      - building_code:建筑在IBMS中编码，非必填，传输设备数据或分项数据时需要
      - sing_list:索要对象标记的列表，可为空，为空时，会索要该建筑下所有对象的数据
      - func_list:索要数据功能码的列表，可为空，为空时，会索要该建筑下所有功能码的数据
    - use_local：1代表读本地库，2代表读远程库
    - data_transfer_time：local_database，读本地库信息
    - wait_time：等待时间，读当前数据时有效，可以指定。如果超过默认等待时间，则使用默认时间。单位分钟。当frequency=5时，wait_time小于10分钟有效，如果大于10分钟，不会等待，一直获取。
    - skip_time：当获取当前时间数据时有效。获取当前时间段数据为空时，会等待skip_time，单位分钟。直到超过skip_time便跳过获取下一段时间数据。
   ```
   ```txt
   get_config为按建筑上传的配置列表，每个建筑对应一个配置，配置说明如下：
    - frequency：数据频率（1：月数据,2：天数据,3：小时数据,4：十分钟数据,5：最近一条数据）
    - value_type：值类型（1：瞬时值，2：累计值，3：计算值，设备equipment/分项runnin  0：其他值）
    - data_type：数据类型（1：原始数据，2：设备数据，3：分项数据，4：平台数据， 5：最近一条数据）
    - table_name_save：data_type=1时有效，本地存数据的表名
    - table_name_get：data_type=1时有效，指定获取那个表的数据
   *注：
      - "原始表结构":"仅支持frequency为5的数据，value_type为0
      - "设备表结构":"仅支持frequency为4的数据",value_type为1,2,3
      -  "分项表结构":"支持frequency为1、2、3、4的数据"，value_type为3或0
      - "平台数据表结构":"支持frequency为1、2、3的数据"，value_type为0
      - "最近一条数据表结构":"仅支持frequency为5的数据，value_type为0，写入最近一条数据表"}

   传输时间会写入data_transfer_time表内，每次上传数据时，会从该表内读取最近一次上传的时间，然后从该时间开始上传数据
   如需更改上传时间，可根据对应建筑、数据频率、值类型、上传或索要标记修改data_transfer_time表内的时间
   ```
3. 运行程序

    ```bash
    python get_data.py
    ```
4. 查看日志

    ```bash
    tail -f DownloadData_logs/get_info.log
    ```