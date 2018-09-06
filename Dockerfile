FROM python:3.6

# These are here for Sentry reporting
ARG GIT_HASH
ENV GIT_HASH=${GIT_HASH}

# Pull in envconsul.
RUN curl -so envconsul.tgz https://releases.hashicorp.com/envconsul/0.7.3/envconsul_0.7.3_linux_amd64.tgz && \
	  tar -xvzf envconsul.tgz && \
	  mv envconsul /usr/local/bin/envconsul && \
	  chmod +x /usr/local/bin/envconsul

RUN mkdir -p /opt/adstxt
ADD ./ /opt/adstxt/

WORKDIR /opt/adstxt/

RUN python setup.py install

CMD ["envconsul", "-consul", "172.17.0.1:8500", "-sanitize", "-upcase", "-prefix", "adstxt/config", "adstxt"] 
