window.addEventListener("DOMContentLoaded", function () {
    var windowPathNameSplits = window.location.pathname.split("/");
    var majorVersionRegex = new RegExp("(\\d+[.]\\d+)");
    var latestRegex = new RegExp("latest");
    if (majorVersionRegex.test(windowPathNameSplits[1])) { // On landing page docs.hopsworks.api/4.0 - URL contains major version
        // Version API dropdown
        document.getElementById("hopsworks_api_link").href = "https://docs.hopsworks.ai/hopsworks-api/" + windowPathNameSplits[1] + "/generated/api/login/";
        document.getElementById("hsfs_javadoc_link").href = "https://docs.hopsworks.ai/hopsworks-api/" + windowPathNameSplits[1] + "/javadoc";
    } else { // on / docs.hopsworks.api/hopsworks-api/4.0
        if (latestRegex.test(windowPathNameSplits[2]) || latestRegex.test(windowPathNameSplits[1])) {
            var majorVersion = "latest";
        } else {
            var apiVersion = windowPathNameSplits[2];
            var majorVersion = apiVersion.match(majorVersionRegex)[0];
        }
        // Version main navigation
        document.getElementsByClassName("md-tabs__link")[0].href = "https://docs.hopsworks.ai/" + majorVersion;
        document.getElementsByClassName("md-tabs__link")[1].href = "https://colab.research.google.com/github/logicalclocks/hopsworks-tutorials/blob/master/quickstart.ipynb";
        document.getElementsByClassName("md-tabs__link")[2].href = "https://docs.hopsworks.ai/" + majorVersion + "/tutorials/";
        document.getElementsByClassName("md-tabs__link")[3].href = "https://docs.hopsworks.ai/" + majorVersion + "/concepts/hopsworks/";
        document.getElementsByClassName("md-tabs__link")[4].href = "https://docs.hopsworks.ai/" + majorVersion + "/user_guides/";
        document.getElementsByClassName("md-tabs__link")[5].href = "https://docs.hopsworks.ai/" + majorVersion + "/setup_installation/aws/getting_started/";
        document.getElementsByClassName("md-tabs__link")[6].href = "https://docs.hopsworks.ai/" + majorVersion + "/admin/";
        // Version API dropdown
        document.getElementById("hopsworks_api_link").href = "https://docs.hopsworks.ai/hopsworks-api/" + majorVersion + "/generated/api/login/";
        document.getElementById("hsfs_javadoc_link").href = "https://docs.hopsworks.ai/hopsworks-api/" + majorVersion + "/javadoc";
    }
});
