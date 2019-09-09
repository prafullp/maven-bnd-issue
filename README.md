# maven-bnd-issue

1. Import project as Maven Project
2. Open devenv.bndrun file to observe the behavior
3. Important breakpoints that I used
	- MavenImplicitProjectRunListener.create(Run run)
	- DefaultRunProvider.create(IResource targetResource, RunMode mode)