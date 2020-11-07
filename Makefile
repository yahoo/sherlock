.PHONY: test jar code-coverage run clean

test:
	mvn clean checkstyle:check javadoc:javadoc test

jar:
	mvn clean checkstyle:check package

jar-no-test:
	mvn -Dmaven.test.skip=true clean checkstyle:check package

code-coverage:
	mvn clean checkstyle:check jacoco:report test

clean:
	mvn clean
