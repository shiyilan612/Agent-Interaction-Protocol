# -*- coding: utf-8 -*-
"""
Created on Tue May 13 14:27:30 2025

@author: haixinwa
"""
import os
from tinydb import TinyDB, Query


class ContextMemory:
    """

    """

    def __init__(self, agent_id, task_id, save_name=None, save_dir="./.cache"):
        self.agent_id = agent_id
        self.task_id = task_id

        os.makedirs(save_dir, exist_ok=True)

    def write(self):
        pass

    def search(self):
        pass

    def listall(self):
        pass

    def update(self):
        pass

    def delete(self):
        pass
