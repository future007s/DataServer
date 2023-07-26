FROM python:3.11-bullseye
# 不会生成__pycache__
# ENV PYTHONDONTWRITEBYTECODE=1
# 设置输出无缓冲
ENV PYTHONUNBUFFERED=1

WORKDIR /app

# 设置pip源为国内源
COPY pip.conf /root/.pip/pip.conf
COPY requirements.txt /app
RUN pip install -r requirements.txt

COPY . /app

# 设置时区
ENV TZ Asia/Shanghai
# 设置中文
ENV LANG C.UTF-8
