# maven-bnd-issue

1. Import project as Maven Project. There might be some compilation errors but you can ignore it
2. Open bnd.bndrun file in cassandra-binding project. It took 43 seconds to open the file using (4.2.0)
3. Important breakpoints that I used
	- MavenImplicitProjectRunListener.create(Run run)
	- DefaultRunProvider.create(IResource targetResource, RunMode mode)