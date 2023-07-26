FROM nginx:1.21.6
#设置时区
ENV TZ Asia/Shanghai
WORKDIR /app
COPY data_reciever.conf /app
COPY nginx.conf /etc/nginx
COPY start.sh /app
