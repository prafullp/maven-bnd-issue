# maven-bnd-issue

1. Import project as Maven Project
2. Open bnd.bndrun file in cassandra-binding project. It took 43 seconds to open the file using (4.2.0)
3. Important breakpoints that I used
	- MavenImplicitProjectRunListener.create(Run run)
	- DefaultRunProvider.create(IResource targetResource, RunMode mode)