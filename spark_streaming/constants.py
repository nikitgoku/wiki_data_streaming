# ------------------------------------------------------------ #
#  Version    : 0.1    							               #
#  Created On : 12-Sept-2024 0:38:39                           #
#  Created By : Nikit Gokhale                                  #
#  Script     : Python                                         #
#  Notes      : Script to parse PostgreSQL configurations      #
# ------------------------------------------------------------ #

import configparser
import os

# This section generates configuration for the PostgreSQL database connection
def load_postgres_config(section='postgreSQL'):
    parser = configparser.ConfigParser()
    parser.read(os.path.join(os.path.dirname(__file__), '../config/config.conf'))
    # Get section default to PostgreSQL
    config = {}
    if parser.has_section(section):
        params = parser.items(section)
        for param in params:
            config[param[0]] = param[1]
    else:
        raise Exception('Section {0} not found in the configuration file'.format(section))
    
    return config

if __name__ == '__main__':
    pg_config = load_postgres_config()
    print(pg_config)