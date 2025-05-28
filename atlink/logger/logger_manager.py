# -*- coding: utf-8 -*-
"""
Created on Thu May 15 09:36:11 2025

@author: clleng
"""

import logging
import logging.handlers
import os
import queue
from pathlib import Path
from typing import List, Dict
from colorlog import ColoredFormatter

class LoggerManager:
    """
    A singleton class to manage logging configuration and provide logger instances.
    This class allows for the creation of loggers with different handlers and levels,
    and ensures that loggers are reused if they already exist.
    It supports various types of handlers including stream, file, rotating file,
    and timed rotating file handlers.
    Attributes:
        log_dir (Path): Directory where log files will be stored, default is 'logs'.
        default_handlers (List[Dict[str, str]]): Default handlers to be used for loggers.
    """
    _instance = None
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance.value = 10  
        return cls._instance
    
    def __init__(self):
        """
        Initialize the LoggerManager.
        """
        if not hasattr(self, '_loggers'):
            self._loggers = {}
            self._listeners = {}
            
            self.log_dir = Path().cwd() / 'logs'
            if not self.log_dir.exists():
                self.log_dir.mkdir(parents=True, exist_ok=True)
            
            self.default_handlers = [
                # console handler
                {
                    'type': 'stream'
                },
                # timed rotating file handler
                {
                    'type': 'timed_rotating_file',
                    'when': 'midnight',
                    'interval': 1,
                    'backupCount': 7
                }
            ]
            
            self.default_formatter = logging.Formatter(
                fmt='%(asctime)s | %(levelname)-8s | %(name)s | %(message)s',
                datefmt='%Y-%m-%d %H:%M:%S'
            )
            
            self.default_colored_formatter = ColoredFormatter(
                fmt='%(asctime)s | %(log_color)s%(levelname)-8s%(reset)s | %(name)s | %(message)s',
                datefmt='%Y-%m-%d %H:%M:%S',
                log_colors={
                    'DEBUG': 'cyan',
                    'INFO': 'green',
                    'WARNING': 'yellow',
                    'ERROR': 'red,bold',
                    'CRITICAL': 'white,bg_red,bold'
                },
                secondary_log_colors={
                    "message": {"WARNING": "yellow", 
                                "ERROR": "red,bold", 
                                "CRITICAL": "white,bg_red,bold"
                    }
                }
            )
            
    
    def get_logger(self, 
                   name: str, 
                   level: str=None, 
                   handlers: List[Dict[str, str]]=None) -> logging.Logger:
        """
        Create or get a logger with the specified name, and set its level and handlers.
        
        Args:
            name: Name of the logger
            level: Logging level (e.g., DEBUG, INFO, WARNING, ERROR, CRITICAL)
            handlers: List of handlers to be added to the logger (e.g., 
            [
                {'type': 'stream', 'level': 'DEBUG', 
                'formatter': {'fmt': '%(asctime)s - %(name)s - %(levelname)s - %(message)s', 'datefmt': '%Y-%m-%d %H:%M:%S'}},
                {'type': 'file', 'filename': 'host.log', 'level': 'INFO'},
                {'type': 'rotating_file', 'filename': 'host_rotating.log', 'maxBytes': 1024 * 1024 * 5, 'backupCount': 5},
                {'type': 'timed_rotating_file', 'filename': 'host_timed.log', 'when': 'midnight', 'interval': 1, 'backupCount': 7}
            ])
            
        Returns:
            logger: Configured logger instance
        """
        
        if name in self._loggers:
            return self._loggers[name]
        
        # Create a directory for the logger if it doesn't exist
        logger_log_dir = self.log_dir / name
        if not logger_log_dir.exists():
            logger_log_dir.mkdir(parents=True, exist_ok=True)
        
        # Create a new logger
        logger = logging.getLogger(name)
        self._loggers[name] = logger
        
        # Set the logger level
        # If level is not provided, use the environment variable or default to INFO
        level = level if level else os.getenv('ATLINK_LOG_LEVEL', 'INFO').upper()
        logger.setLevel(level)
        
        self.set_handlers(logger, name, handlers, logger_log_dir)
        
        # logger.info(f"<Logger>: Logger '{name}' started with level {level}")
                
        return logger
    
    def set_handlers(self, 
                     logger: logging.Logger, 
                     name: str, 
                     handlers: List[Dict[str, str]]=None, 
                     log_dir: Path=None):
        """
        Set handlers for the logger.
        Args:
            logger: Logger instance to set handlers for
            name: Name of the logger
            handlers: List of handlers to be added to the logger (e.g., 
            [
                {'type': 'stream', 'level': 'DEBUG', 
                'formatter': {'fmt': '%(asctime)s - %(name)s - %(levelname)s - %(message)s', 'datefmt': '%Y-%m-%d %H:%M:%S'}},
                {'type': 'file', 'filename': 'host.log', 'level': 'INFO'},
                {'type': 'rotating_file', 'filename': 'host_rotating.log', 'maxBytes': 1024 * 1024 * 5, 'backupCount': 5},
                {'type': 'timed_rotating_file', 'filename': 'host_timed.log', 'when': 'midnight', 'interval': 1, 'backupCount': 7}
            ])
            log_dir: Directory where log files will be stored
        """
        log_queue = queue.Queue()

        handler_objects = []
        
        if handlers is None:
            handlers = self.default_handlers
        for handler_config in handlers:
            handler_type = handler_config.get('type')
            if handler_type == 'stream':
                handler = logging.StreamHandler()
            elif handler_type == 'file':
                filename = log_dir / handler_config.get('filename', f'{name}.log')
                handler = logging.FileHandler(str(filename))
            elif handler_type == 'rotating_file':
                filename = log_dir / handler_config.get('filename', f'{name}_rotating.log')
                max_bytes = handler_config.get('maxBytes', 1024 * 1024 * 5)
                backup_count = handler_config.get('backupCount', 5)
                handler = logging.handlers.RotatingFileHandler(
                    str(filename), maxBytes=max_bytes, backupCount=backup_count
                )
            elif handler_type == 'timed_rotating_file':
                filename = log_dir / handler_config.get('filename', f'{name}_timed.log')
                when = handler_config.get('when', 'midnight')
                interval = handler_config.get('interval', 1)
                backup_count = handler_config.get('backupCount', 7)
                handler = logging.handlers.TimedRotatingFileHandler(
                    str(filename), when=when, interval=interval, backupCount=backup_count
                )
            else:
                raise ValueError(f"<LoggerManager>: Unsupported handler type: {handler_type}")
            
            # Set handler level
            handler_level = handler_config.get('level', 'NOTSET').upper()
            handler.setLevel(handler_level)
            
            # Set formatter
            self.set_formatter(handler, handler_config)
          
            handler_objects.append(handler)
            logger.addHandler(handler)
            
        # listener = logging.handlers.QueueListener(log_queue, *handler_objects)
        # if name not in self._listeners:
        #     self._listeners[name] = listener
        # listener.start()
            
        # queue_handler = logging.handlers.QueueHandler(log_queue)
        # logger.addHandler(queue_handler)
        
    def set_formatter(self, handler: logging.Handler, handler_config: Dict[str, str]):
        """
        Set formatter for the specified handler.
        
        Args:
            handler: Handler instance to set formatter for
            handler_config: Configuration for the formatter
        """
        if 'formatter' in handler_config:
            formatter_args = handler_config['formatter']
            formatter = logging.Formatter(**formatter_args)
            handler.setFormatter(formatter)
        else:
            handler_type = handler_config.get('type')
            if handler_type == 'stream':
                handler.setFormatter(self.default_colored_formatter)
            else:
                handler.setFormatter(self.default_formatter)
        
    def stop_listener(self, name: str):
        """
        Stop the listener for the specified logger.
        
        Args:
            name: Name of the logger
        """
        if name in self._listeners:
            listener = self._listeners[name]
            listener.stop()
            del self._listeners[name]
            if name in self._loggers:
                del self._loggers[name]
                
    def stop(self):
        """
        Stop all listeners and clear the loggers.
        """
        for name in list(self._listeners.keys()):
            self.stop_listener(name)
        self._loggers.clear()
        self._listeners.clear()
    