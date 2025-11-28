# GitRemote API

You can obtain a `GitApi` handle via [`Project.get_git_api`][hopsworks.project.Project.get_git_api].
Once you have an API handle, you can use a [`GitRepo`][hopsworks.git_repo.GitRepo] object to create (via [`GitRepo.add_remote`][hopsworks.git_repo.GitRepo.add_remote]) or retrieve (via [`GitRepo.get_remote`][hopsworks.git_repo.GitRepo.get_remote] or [`GitRepo.get_remotes`][hopsworks.git_repo.GitRepo.get_remotes]) `GitRemote` objects.

::: hopsworks.git_remote.GitRemote
