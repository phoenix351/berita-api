CREATE TABLE `corona_twit` (
  `status_posted` datetime NOT NULL,
  `user_desc` longtext,
  `user_status_count` int(11) NOT NULL,
  `user_name` varchar(25) NOT NULL,
  `user_target_reply` varchar(25) DEFAULT NULL,
  `user_erified` varchar(6) NOT NULL,
  `status_text` text NOT NULL,
  `status_hashtags` text,
  `user_location` varchar(50) DEFAULT NULL,
  `user_following` int(11) NOT NULL,
  `user_followers` int(11) NOT NULL,
  `retweets` int(11) NOT NULL,
  `coordinate` text,
  `country_code` varchar(3) DEFAULT NULL
);


ALTER TABLE `corona_twit`
  ADD PRIMARY KEY (`status_posted`,`user_status_count`,`user_name`);
COMMIT;
