// Java/Scala build for the hsfs SDK jars. Split out of the `hopsworks` freestyle job because
// main and branch-5.1 need JDK 17 (Spark 4.1, avro 1.12.x) while branch-4.* and branch-5.0
// stay on the controller's Java 8. The controller has no JDK 17 installation, so the build
// runs in a container the way clusterj-onlinefs and hopsworks-ee already do; the publish step
// has to run back on `local`, because /opt/repository is the controller's filesystem.
pipeline {
    agent none

    options {
        buildDiscarder(logRotator(numToKeepStr: '10'))
        // The freestyle job this replaces had concurrentBuild=false. Two runs in parallel
        // would install the same GAV into the shared ~/.m2 and write the same
        // /opt/repository/master/hsfs/$POM_VERSION directory.
        disableConcurrentBuilds()
    }

    triggers {
        githubPush()
    }

    stages {
        stage('build') {
            agent {
                docker {
                    image 'maven:3.8.5-openjdk-17'
                    label 'local'
                    // The .m2 mount carries the controller's settings.xml and its warm local
                    // repository into the container, which is what supplies the mirror and the
                    // archiva deploy credentials. /root/.m2 cannot deliver either, being 0700 to
                    // a non-root container, so mount somewhere traversable and move user.home
                    // with it, the way the maven image documents for -u.
                    // The passwd/group mounts are what make that uid resolvable at all. The
                    // plugin runs the container as the agent's uid, which the image has no entry
                    // for, and everything downstream of getpwuid() fails: Maven read no settings
                    // (user.home=?), and Hadoop's UserGroupInformation login died on
                    // `new UnixPrincipal(null)`, failing every test that builds a SparkSession
                    // with "Invalid UID, could not determine effective user".
                    args '-v $HOME/.m2:/var/maven/.m2 -e HOME=/var/maven -e MAVEN_CONFIG=/var/maven/.m2 -v /etc/passwd:/etc/passwd:ro -v /etc/group:/etc/group:ro'
                }
            }
            environment {
                // getpwuid() resolves now, so the JVM takes user.home from the controller's
                // passwd entry, a /home path that is not mounted in. $HOME is still ignored, so
                // -D remains the only thing that moves it, for every mvn call in the stage.
                MAVEN_OPTS = '-Duser.home=/var/maven'
            }
            steps {
                script {
                    env.POM_VERSION = sh(returnStdout: true, script:
                        "mvn -f java/pom.xml -q -Dexec.executable=echo -Dexec.args='\${project.version}' --non-recursive exec:exec").trim()
                    // Derived, never hardcoded: this is the string that was pinned to spark3.5
                    // in the old job, which is why no spark4.1 jar was ever published.
                    env.SPARK_FLAVOUR = sh(returnStdout: true, script:
                        "mvn -f java/pom.xml -q -Dexec.executable=echo -Dexec.args='\${artifact.spark.version}' --non-recursive exec:exec").trim()
                    echo "POM_VERSION=${env.POM_VERSION}  SPARK_FLAVOUR=${env.SPARK_FLAVOUR}"
                }
                sh 'mvn -f java/pom.xml clean deploy generate-sources javadoc:javadoc -Pwith-hops-ee'
                sh 'mvn -f utils/java/pom.xml clean package'
                stash name: 'artifacts', includes: [
                    'java/spark/target/*-jar-with-dependencies.jar',
                    'utils/java/target/*-jar-with-dependencies.jar',
                    'utils/python/hsfs_utils.py',
                ].join(',')
            }
        }

        stage('publish') {
            agent { label 'local' }
            options { skipDefaultCheckout() }
            steps {
                deleteDir()
                unstash 'artifacts'
                sh '''
                    set -eu
                    DEST=/opt/repository/master
                    mkdir -p "$DEST/hsfs/$POM_VERSION" "$DEST/hsfs_utils"

                    JAR="java/spark/target/hsfs-spark-$SPARK_FLAVOUR-$POM_VERSION-jar-with-dependencies.jar"
                    # Fatal, not a soft `if`. The old job skipped silently when the name did not
                    # match, which is how main went green while publishing nothing.
                    test -f "$JAR"
                    cp "$JAR" "$DEST/hsfs/$POM_VERSION/hsfs-spark-$SPARK_FLAVOUR-$POM_VERSION.jar"

                    cp "utils/java/target/hsfs-utils-$POM_VERSION-jar-with-dependencies.jar" \
                       "$DEST/hsfs_utils/hsfs-utils-$POM_VERSION.jar"
                    cp utils/python/hsfs_utils.py "$DEST/hsfs_utils/hsfs_utils-$POM_VERSION.py"
                '''
            }
        }
    }

    post {
        success {
            build job: 'k8s-base-images', wait: false
        }
        unstable  { slackSend color: 'warning', message: "${env.JOB_NAME} #${env.BUILD_NUMBER} unstable: ${env.BUILD_URL}" }
        failure   { slackSend color: 'danger',  message: "${env.JOB_NAME} #${env.BUILD_NUMBER} failed: ${env.BUILD_URL}" }
        fixed     { slackSend color: 'good',    message: "${env.JOB_NAME} #${env.BUILD_NUMBER} back to normal: ${env.BUILD_URL}" }
    }
}
