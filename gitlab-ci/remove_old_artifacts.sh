#!/bin/bash

AGE_DAYS=10
EOS_DIRS=/eos/project/s/storage-ci/www/eos/citrine/commit

for commit_dir in ${EOS_DIRS}/*; do
  count=$(find ${commit_dir} -regex '.*\(rpm\|dmg\)$' -type f -mtime +${AGE_DAYS} -print -delete | wc -l)

  if [[ ${count} -gt 0 ]]; then
    echo "${commit_dir}: deleted ${count} RPMs"

    # Recreate YUM repos
    yum_repos=$(find ${commit_dir} -path "*/SRPMS" -o -path "*/x86_64" -type d)

    for repo_dir in ${yum_repos[@]}; do
      echo "Updating YUM repo: ${repo_dir}"
      createrepo --update -q ${repo_dir}
    done
  fi
done
