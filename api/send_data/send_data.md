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
    - send_url：上传数据的url地址
    - login_url：登录的url地址
   ```
   ```txt
   send_config为按建筑上传的配置列表，每个建筑对应一个配置，配置说明如下：
    - building_id:建筑id
    - building_code:建筑在IBMS中编码，非必填，传输设备数据或分项数据时需要
    - frequency：数据频率（1：月数据,2：天数据,3：小时数据,4：十分钟数据,5：最近一条数据，0：其他数据）
    - value_type：值类型（1：瞬时值，2：累计值，0：其他值）
    - data_type：数据类型（1：原始数据，2：设备数据，3：分项数据，0：其他数据）
   *注：原始数据仅有数据频率为0可用，设备数据仅有数据频率为4和5（待完善）可用，分项数据仅有数据频率为1、2、3、4可用
   传输时间会写入data_transfer_time表内，每次上传数据时，会从该表内读取最近一次上传的时间，然后从该时间开始上传数据
   如需更改上传时间，可根据对应建筑、数据频率、值类型、上传或索要标记修改data_transfer_time表内的时间
   ```
3. 运行程序

    ```bash
    python send_data.py
    ```
4. 查看日志

    ```bash
    tail -f UploadData_logs/send_info.log
    ```