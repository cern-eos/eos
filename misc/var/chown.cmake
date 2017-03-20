# Copy the files to the destination directory
EXECUTE_PROCESS(COMMAND chown daemon /var/eos/wfe/bash/shell
                COMMAND chgrp daemon /var/eos/wfe/bash/shell)

