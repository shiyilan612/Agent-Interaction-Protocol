# -*- coding: utf-8 -*-
"""
Created on Tue May 13 14:27:30 2025

@author: haixinwa
"""
import os
import time
import logging
from typing import Union
from tinydb import TinyDB
from ..grpc_service.type import AgentMessage, ToolRequest, ToolResponse


class ContextMemory:
    """

    """

    def __init__(self, agent_id, save_root="./.cache"):
        self.agent_id = agent_id
        self.save_dir = os.path.join(save_root, f"{self.agent_id}_{time.strftime('%Y%m%d_%H%M%S', time.localtime())}")
        os.makedirs(self.save_dir, exist_ok=True)
        self.dbs = dict()

    def create(self, task_id):
        save_path = f"{os.path.join(self.save_dir, task_id)}.json"
        if os.path.exists(save_path):
            logging.error(f"{save_path} is already existed")
            return False
        try:
            new_db = TinyDB(save_path)
            self.dbs[task_id] = new_db
            return True
        except Exception as e:
            logging.error(e)
            return False

    def write(self, task_id, message: Union[AgentMessage, ToolRequest, ToolResponse]):
        db = self.dbs.get(task_id)
        if db is None:
            logging.error(f"Memory of {task_id} is not existed")
            return False
        else:
            db.insert(message.to_dict())

    def search(self, task_id, condtion):
        db = self.dbs.get(task_id)
        if db:
            return db.search(condtion)
        else:
            logging.error(f"Memory of {task_id} is not existed")
            return None

    def listall(self, task_id):
        db = self.dbs.get(task_id)
        if db:
            return db.all()
        else:
            logging.error(f"Memory of {task_id} is not existed")
            return None

    def update(self):
        pass

    def delete(self):
        pass

    def clear(self, task_id):
        db = self.dbs.get(task_id)
        if db is None:
            logging.error(f"Memory of {task_id} is not existed")
            return False
        else:
            db.truncate()
            return True
