heroku ps:scale clock=0 && ^
heroku stop clock.1 && ^
heroku ps:scale clock=1 && ^
heroku restart clock.1
