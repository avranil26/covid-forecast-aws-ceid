FROM public.ecr.aws/lambda/provided

ENV R_VERSION=4.0.3

##ENV TAR = "/bin/tar"

RUN yum -y install wget
RUN yum -y install unzip
RUN yum -y install tar

RUN yum -y install https://dl.fedoraproject.org/pub/epel/epel-release-latest-7.noarch.rpm \
  && wget https://cdn.rstudio.com/r/centos-7/pkgs/R-${R_VERSION}-1-1.x86_64.rpm \
  && yum -y install R-${R_VERSION}-1-1.x86_64.rpm \
  && rm R-${R_VERSION}-1-1.x86_64.rpm

ENV PATH="${PATH}:/opt/R/${R_VERSION}/bin/"

# System requirements for R packages
RUN yum -y install openssl-devel
RUN yum -y install libxml2-devel

RUN Rscript -e "install.packages(c('httr', 'curl', 'xml2', 'jsonlite', 'logger', 'aws.s3', 'tidyverse', 'remotes', 'devtools', 'arrow'), repos = 'https://cloud.r-project.org/')"
#RUN Rscript -e "install.packages(c('httr', 'curl', 'xml2', 'jsonlite', 'logger', 'aws.s3', 'tidyverse', 'remotes'), repos = 'https://cloud.r-project.org/'); remotes::install_github('e3bo/epidatr', force = FALSE, upgrade = TRUE, dependencies= TRUE, lib = '${PATH}:/opt/R/${R_VERSION}/lib/')"
#RUN ln -s /bin/tar /bin/gtar
RUN Rscript -e "remotes::install_github('e3bo/epidatr', force = FALSE, upgrade = TRUE, dependencies= TRUE, lib = ${R_LIBS_USER})"

RUN Rscript -e "devtools::install_github('reichlab/zoltr')"
RUN Rscript -e "devtools::install_github('reichlab/covidData')"


RUN Rscript -e "devtools::install_github('reichlab/covidHubUtils')"


#RUN 'cd $PATH\nRscript -e "remotes::install_github("e3bo/epidatr")'
#RUN Rscript -e '#!/bin/sh\ncd user_lib <- Sys.getenv("R_LIBS_USER"); message("user_lib = ", user_lib); remotes::install_github("e3bo/epidatr", force = FALSE, upgrade = TRUE, lib = user_lib)'

COPY runtime.R functions.R ${LAMBDA_TASK_ROOT}/
RUN chmod 755 -R ${LAMBDA_TASK_ROOT}/

RUN printf '#!/bin/sh\ncd $LAMBDA_TASK_ROOT\nRscript runtime.R' > /var/runtime/bootstrap \
  && chmod +x /var/runtime/bootstrap

