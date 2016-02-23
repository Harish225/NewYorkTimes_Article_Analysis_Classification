package com.unm.wordStemming;

public class Stem {
	
	public static void main(String[] args) throws Throwable {
		System.out.println(stemWord("performance"));
	}

	public static String stemWord(String input) throws Throwable {

		Class<?> stemClass = Class.forName("com.unm.wordStemming.EnglishStemmer");
		SnowballStemmer stemmer = (SnowballStemmer) stemClass.newInstance();

		if (input.length() > 0) {
			SnowballProgram.setCurrent(input);
			stemmer.stem();
			return stemmer.getCurrent();

		} else {
			return input;
		}
	}
}