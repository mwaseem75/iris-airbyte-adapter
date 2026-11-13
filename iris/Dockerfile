FROM intersystems/iris-community:latest-em

USER root
COPY --chown=irisowner:irisowner iris.script /home/irisowner/iris.script
COPY --chown=irisowner:irisowner --chmod=755 onstart.sh /home/irisowner/onstart.sh
USER irisowner

# One-time setup baked into the image: enable Interoperability on USER, unexpire passwords.
RUN iris start IRIS \
 && iris session IRIS -U %SYS < /home/irisowner/iris.script \
 && iris stop IRIS quietly
