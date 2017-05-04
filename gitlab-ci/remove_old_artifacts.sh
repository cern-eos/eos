#! /bin/bash

AGE_DAYS=10
EOS_DIRS=/eos/project/s/storage-ci/www/eos/*/commit/

for commit_dir in ${EOS_DIRS}; do
  find ${commit_dir} -regex '.*\(rpm\|dmg\)$' -type f -mtime +${AGE_DAYS} -exec rm {} \; || true
  # Recreate YUM repos
  yum_repos=$(find ${commit_dir} -path "*/i386" -o -path "*/x86_64" -type d)                                                                                                                                                           
                                                                                                                                                                                                                                       
  for repo_dir in ${yum_repos[@]}; 
  do                                                                                                                                                                                                      
    echo "Update YUM repo: ${repo_dir}"
    createrepo --update -q ${repo_dir}
  done
done                                                                                                                                                         


