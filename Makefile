
APP_NAME = clustermgr
REMOTE_USER = era
REMOTE_HOST ?= 10.117.81.164
PORT_TO_RUN_ON ?= 5000
REMOTE_DIR = /home/era/$(APP_NAME)

CLUSTER_CFG_FILE ?= clusters.yml
USER_CFG_FILE ?= users.yaml
CACHE_REFRESH_INTERVAL ?= 3h
MAIL_FREQUENCY ?= @1000
ACTION_FREQUENCY ?= @1700
REFRESH_DEVIATION ?= 3h
RETAIN_DEVIATIONS ?= 7d
DEVIATION_CHECKBACK_INTERVAL ?= 2d

PYTHON = python3.12

.PHONY: help init_prod init_eval deploy stop clean all

deploy:
	@echo "Deploying to $(REMOTE_USER)@$(REMOTE_HOST):$(REMOTE_DIR)"
		ssh $(REMOTE_USER)@$(REMOTE_HOST) "\
			mkdir -p $(REMOTE_DIR) && \
			sudo dnf install $(PYTHON) -y"
		rsync -av --exclude='*.pyc' --exclude='venv/' --exclude='*.log' --exclude='.git/' --exclude='.idea/' --exclude='.vscode/' --exclude='*/__pycache__' ./ $(REMOTE_USER)@$(REMOTE_HOST):$(REMOTE_DIR)/
	@echo "Deployed to $(REMOTE_HOST):$(REMOTE_DIR)"

init_eval_local:
	$(PYTHON) clustermgr.py $(CLUSTER_CFG_FILE) $(USER_CFG_FILE) \
	--port $(PORT_TO_RUN_ON) \
	--refresh-cache $(CACHE_REFRESH_INTERVAL) \
	--mail-frequency $(MAIL_FREQUENCY) \
	--action-frequency $(ACTION_FREQUENCY) \
	--refresh-deviations $(REFRESH_DEVIATION) \
	--retain-deviations $(RETAIN_DEVIATIONS) \
	--deviations-checkback $(DEVIATION_CHECKBACK_INTERVAL) \
	--eval > flask.log 2>&1 &

init_prod:
	@echo "Initializing $(APP_NAME) on $(REMOTE_HOST) for user $(REMOTE_USER)"
	ssh $(REMOTE_USER)@$(REMOTE_HOST) "\
		cd $(REMOTE_DIR) && \
		$(PYTHON).12 -m venv venv && \
		source venv/bin/activate && \
		pip install --upgrade pip && \
		pip install -r requirements.txt && \
		nohup $(PYTHON) clustermgr.py $(CLUSTER_CFG_FILE) $(USER_CFG_FILE) \
		--port $(PORT_TO_RUN_ON) \
		--refresh-cache $(CACHE_REFRESH_INTERVAL) \
		--mail-frequency $(MAIL_FREQUENCY) \
		--action-frequency $(ACTION_FREQUENCY) \
		--refresh-deviations $(REFRESH_DEVIATION) \
		--retain-deviations $(RETAIN_DEVIATIONS) \
		--deviations-checkback $(DEVIATION_CHECKBACK_INTERVAL) > flask.log 2>&1 & \
		sleep 5" > flask.log 2>&1 &
	@echo "Initialized $(APP_NAME) on $(REMOTE_HOST) for user $(REMOTE_USER)"

init_eval:
	@echo "Initializing $(APP_NAME) on $(REMOTE_HOST) for user $(REMOTE_USER) in EVAL mode"
	sshpass -p Nutanix.1 ssh $(REMOTE_USER)@$(REMOTE_HOST) "\
		cd $(REMOTE_DIR) && \
		$(PYTHON) -m venv venv && \
		source venv/bin/activate && \
		pip install --upgrade pip && \
		pip install -r requirements.txt && \
		nohup $(PYTHON) clustermgr.py $(CLUSTER_CFG_FILE) $(USER_CFG_FILE) \
		--port $(PORT_TO_RUN_ON) \
		--refresh-cache $(CACHE_REFRESH_INTERVAL) \
		--mail-frequency $(MAIL_FREQUENCY) \
		--action-frequency $(ACTION_FREQUENCY) \
		--refresh-deviations $(REFRESH_DEVIATION) \
		--retain-deviations $(RETAIN_DEVIATIONS) \
		--deviations-checkback $(DEVIATION_CHECKBACK_INTERVAL) \
		--eval > flask.log 2>&1 & \
		sleep 5" > flask.log 2>&1 &
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
	sudo curl -s http://$(REMOTE_HOST):$(PORT_TO_RUN_ON)/
	@echo ""
	@echo "Verified $(APP_NAME) on $(REMOTE_HOST)"
