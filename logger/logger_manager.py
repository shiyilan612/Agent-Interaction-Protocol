import logging
import logging.config
import logging.handlers
import yaml
import importlib
from pathlib import Path

class LoggerManager:
    def __init__(self, config_path=None):
        """
        Initialize the LoggerManager.
        Args:
            config_path: Path to the YAML configuration file
        """
        if not config_path:
            config_path = Path(__file__).resolve().parent / 'logging.yaml'
            
        self.log_dir = Path(__file__).resolve().parent.parent / 'logs'
        if not self.log_dir.exists():
            self.log_dir.mkdir(parents=True, exist_ok=True)
        
        if config_path.exists():
            with open(config_path, 'r', encoding = 'utf-8') as f:
                config = yaml.safe_load(stream=f)
                        
            logging.config.dictConfig(config)
        else:
            raise FileNotFoundError(f"Logging config file not found: {config_path}")
        
        self.handlers = {}
        for handler_name, handler_config in config.get('handlers', {}).items():
            self.handlers[handler_name] = handler_config.copy()
            
        self.formatters = {}
        for formatter_name, formatter_config in config.get('formatters', {}).items():
            self.formatters[formatter_name] = formatter_config.copy()
            
    def get_logger(self, name=__name__):
        
        if name in logging.Logger.manager.loggerDict:
            return logging.getLogger(name)
            
        logger = logging.getLogger(name)
        
        logger_log_dir = self.log_dir / name
        if not logger_log_dir.exists():
            logger_log_dir.mkdir(parents=True, exist_ok=True)
            
        log_filename = logger_log_dir / f"{name}.log"

        handler = self.create_handler('host_handler', str(log_filename))
        logger.addHandler(handler)

        logger.setLevel(logging.INFO)
        
        return logger
    
    def create_handler(self, handler_name, finename):
        """
        Create a new handler based on the specified handler name.
        Args:
            handler_name: Name of the handler to create
            filename: Filename for the handler
        """
        if handler_name in self.handlers:
            handler_config = self.handlers[handler_name]
            
            module_path, class_name = handler_config['class'].rsplit('.', 1)
            module = importlib.import_module(module_path)
            handler_class = getattr(module, class_name)
            
            if handler_class == logging.handlers.TimedRotatingFileHandler:
                handler = handler_class(
                    filename=finename,
                    when=handler_config['when'],
                    interval=handler_config['interval'],
                    backupCount=handler_config['backupCount'],
                    encoding=handler_config.get('encoding', 'utf-8'),
                    delay=handler_config.get('delay', False)
                )
                handler.setLevel(getattr(logging, handler_config['level']))
                handler.setFormatter(self.create_formatter(handler_config['formatter']))
                
                return handler
            else:
                raise ValueError(f"Unsupported handler class: {handler_class}")
        else:
            raise ValueError(f"Handler {handler_name} not found in configuration.")
    
    def create_formatter(self, formatter_name):
        """
        Create a new formatter based on the specified formatter name.
        Args:
            formatter_name: Name of the formatter to create
        """
        if formatter_name in self.formatters:
            formatter_config = self.formatters[formatter_name]
            return logging.Formatter(fmt=formatter_config['format'], 
                                     datefmt=formatter_config.get('datefmt', None))
        else:
            raise ValueError(f"Formatter {formatter_name} not found in configuration.")
    