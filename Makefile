
APP_NAME = clustermgr
REMOTE_USER = root
REMOTE_HOST ?= 10.117.81.174
PORT_TO_RUN_ON ?= 5000
REMOTE_DIR = /tmp/$(APP_NAME)

CLUSTER_CFG_FILE ?= clusters.yml
USER_CFG_FILE ?= users.yaml
CACHE_REFRESH_INTERVAL ?= 2h
MAIL_FREQUENCY ?= @1000
ACTION_FREQUENCY ?= @1700
REFRESH_OFFENSE ?= 4h
RETAIN_OFFENSES ?= 7d
OFFENSE_CHECKBACK_INTERVAL ?= 2d

.PHONY: help init_prod init_eval deploy stop clean all

deploy:
	@echo "Deploying to $(REMOTE_USER)@$(REMOTE_HOST):$(REMOTE_DIR)"
		ssh $(REMOTE_USER)@$(REMOTE_HOST) "\
			mkdir -p $(REMOTE_DIR) && \
			dnf install python3.12 -y"
		rsync -av --exclude='*.pyc' --exclude='venv/' --exclude='*.log' --exclude='.git/' --exclude='.idea/' --exclude='.vscode/' --exclude='*/__py__cache' ./ $(REMOTE_USER)@$(REMOTE_HOST):$(REMOTE_DIR)/
	@echo "Deployed to $(REMOTE_HOST):$(REMOTE_DIR)"

init_eval_local:
	python3 clustermgr.py $(CLUSTER_CFG_FILE) $(USER_CFG_FILE) \
	--port $(PORT_TO_RUN_ON) \
	--refresh-cache $(CACHE_REFRESH_INTERVAL) \
	--mail-frequency $(MAIL_FREQUENCY) \
	--action-frequency $(ACTION_FREQUENCY) \
	--refresh-offense $(REFRESH_OFFENSE) \
	--retain-offense $(RETAIN_OFFENSES) \
	--offense-checkback $(OFFENSE_CHECKBACK_INTERVAL) \
	--eval

init_prod:
	@echo "Initializing $(APP_NAME) on $(REMOTE_HOST) for user $(REMOTE_USER)"
	ssh $(REMOTE_USER)@$(REMOTE_HOST) "\
		cd $(REMOTE_DIR) && \
		python3.12 -m venv venv && \
		source venv/bin/activate && \
		pip install --upgrade pip && \
		pip install -r requirements.txt && \
		nohup python3 clustermgr.py $(CLUSTER_CFG_FILE) $(USER_CFG_FILE) \
		--port $(PORT_TO_RUN_ON) \
		--refresh-cache $(CACHE_REFRESH_INTERVAL) \
		--mail-frequency $(MAIL_FREQUENCY) \
		--action-frequency $(ACTION_FREQUENCY) \
		--refresh-offense $(REFRESH_OFFENSE) \
		--retain-offense $(RETAIN_OFFENSES) \
		--offense-checkback $(OFFENSE_CHECKBACK_INTERVAL) > flask.log 2>&1 & \
		sleep 5""
	@echo "Initialized $(APP_NAME) on $(REMOTE_HOST) for user $(REMOTE_USER)"

init_eval:
	@echo "Initializing $(APP_NAME) on $(REMOTE_HOST) for user $(REMOTE_USER) in EVAL mode"
	ssh $(REMOTE_USER)@$(REMOTE_HOST) "\
		cd $(REMOTE_DIR) && \
		python3 -m venv venv && \
		source venv/bin/activate && \
		pip install --upgrade pip && \
		pip install -r requirements.txt && \
		nohup python3 clustermgr.py $(CLUSTER_CFG_FILE) $(USER_CFG_FILE) \
		--port $(PORT_TO_RUN_ON) \
		--refresh-cache $(CACHE_REFRESH_INTERVAL) \
		--mail-frequency $(MAIL_FREQUENCY) \
		--action-frequency $(ACTION_FREQUENCY) \
		--refresh-offense $(REFRESH_OFFENSE) \
		--retain-offense $(RETAIN_OFFENSES) \
		--offense-checkback $(OFFENSE_CHECKBACK_INTERVAL) \
		--eval > flask.log 2>&1 & \
		sleep 5"
	@echo "Initialized $(APP_NAME) on $(REMOTE_HOST) for user $(REMOTE_USER)"

stop:
	@echo "Stopping $(APP_NAME) on $(REMOTE_HOST)"
	ssh $(REMOTE_USER)@$(REMOTE_HOST) "pkill -f $(APP_NAME)"
	@echo "Stopped $(APP_NAME) on $(REMOTE_HOST)"

clean:
	@echo "Cleaning up $(APP_NAME) on $(REMOTE_HOST)"
	ssh $(REMOTE_USER)@$(REMOTE_HOST) "\
		cd $(REMOTE_DIR) && \
		deactivate && \
		rm -rf $(REMOTE_DIR)"
	@echo "Cleaned up $(APP_NAME) on $(REMOTE_HOST)"

all: deploy init_eval

verify:
	@echo "Verifying $(APP_NAME) on $(REMOTE_HOST)"
	curl -s http://$(REMOTE_HOST):$(PORT_TO_RUN_ON)/
	@echo ""  # Add a new line
	@echo "Verified $(APP_NAME) on $(REMOTE_HOST)"
