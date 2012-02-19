
<!DOCTYPE html>
<html lang="en">
  <head>
    <meta charset="utf-8">
    <title>Fluxtream - Personal Analytics</title>
    <meta name="description" content="">
    <meta name="author" content="">

    <!-- Le HTML5 shim, for IE6-8 support of HTML elements -->
    <!--[if lt IE 9]>
      <script src="http://html5shim.googlecode.com/svn/trunk/html5.js"></script>
    <![endif]-->

    <!-- Le styles -->
	<link rel="stylesheet/less" href="/static/less/bootstrap.less">
	<link rel="stylesheet/less" href="/${release}/css/flx.less">
	<script src="/static/js/less-1.2.1.min.js"></script>
    <style>
      body {
        padding-top: 60px; /* 60px to make the container go all the way to the bottom of the topbar */
      }
    </style>
    
    <link href="/static/css/bootstrap-responsive-2.0.css" rel="stylesheet">

    <!-- Le fav and touch icons -->
    <link rel="shortcut icon" href="/favicon.ico">
    
  </head>

  <body>
	<a href="https://github.com/fluxtream/fluxtream-app"><img style="position: absolute; top: 0; right: 0; border: 0;" src="http://s3.amazonaws.com/github/ribbons/forkme_right_darkblue_121621.png" alt="Fork me on GitHub"></a>
    <div class="container">


<div class="row">
    <div class="span6">&nbsp;
    </div>
    <div class="span4">

		<form class="well" action="signIn" method="POST">
			
			<label for="user_email">Username</label><br>
			<input autocomplete="on" class="span3" onkeypress="if(event.which==13) document.forms[0].submit();"
				class="title" id="f_username" name="f_username"
				value="<%=request.getParameter("username")!=null?request.getParameter("username"):"" %>"
			type="text"><br>
		
			<label for="user_password">Password</label><br>
			<input value="" class="span3" onkeypress="if(event.which==13) document.forms[0].submit();"
				class="title" id="f_password" name="f_password"
				type="password" style="margin-bottom:20px;">
			<br/>
			<input type="submit" class="btn" id="signinButton"  class="btn primary large" style="width:100px;" value="&raquo; Sign In"/> &nbsp;
			<a href="support/lostPassword">&raquo; Password forgotten?</a>
		</form>

    </div>
    <div class="span1">&nbsp;
    </div>
  </div>

    </div> <!-- /container -->

    <!-- Le javascript
    ================================================== -->
    <!-- Placed at the end of the document so the pages load faster -->
	<script src="//ajax.googleapis.com/ajax/libs/jquery/1.7.1/jquery.min.js"></script>
	<script>window.jQuery || document.write('<script src="js/libs/jquery-1.7.1.min.js"><\/script>')</script>
    <script src="/static/js/bootstrap-2.0.min.js"></script>
    <script src="/${release}/js/welcome.js"></script>

  </body>
</html>
