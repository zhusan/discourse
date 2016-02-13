require "set"
require "active_support"
require "active_support/core_ext"

class SpamClassifier

  TOKEN_REGEX ||= /(?:\d+[.,])+\d+|[[[:alnum:]]_\-']+/
  ONLY_DIGIT_REGEX ||= /^\d+$/

  SPAM_PROBABILITY_THRESHOLD ||= 0.9
  UNKNOWN_WORD_SPAM_PROBABILITY ||= 0.4

  HAM_WEIGHT ||= 5
  OCCURENCE_THRESHOLD ||= 30

  def initialize
    @all_words = Set.new
    @documents = { spam: 0, ham: 0 }
    @word_counts = { spam: Hash.new(0), ham: Hash.new(0) }
    @spamicities = Hash.new(UNKNOWN_WORD_SPAM_PROBABILITY)
  end

  def train(category, document)
    @documents[category] += 1
    tokenize(document).each do |word|
      @all_words << word
      @word_counts[category][word] += 1
    end
  end

  def summarize!
    hams = @documents[:ham].to_f
    spams = @documents[:spam].to_f

    min_probability = (1 * 10 ** -(@documents.values.reduce(:+).to_s.size)).to_f
    max_probability = 1 - min_probability

    @all_words.each do |word|
      h = (HAM_WEIGHT * @word_counts[:ham][word]).to_f
      s = @word_counts[:spam][word].to_f

      next if h + s < OCCURENCE_THRESHOLD

      p_s = [1.0, s / spams].min
      p_h = [1.0, h / hams].min
      p = p_s / (p_s + p_h)
      p = [p, max_probability].min
      p = [p, min_probability].max

      @spamicities[word] = p
    end
  end

  def is_spam?(document)
    # tokenize
    tokens = tokenize(document).map { |t| [t, interestingeness(t)] }
    # keep most 15 interesting
    most_interesting_tokens = tokens.sort { |a, b| b[1] <=> a[1] }.take(15)
    # is spam if combined probability > .9
    spamicities = most_interesting_tokens.map { |t| @spamicities[t[0]] }
    combined_probability(spamicities) > SPAM_PROBABILITY_THRESHOLD
  end

  private

    def tokenize(document)
      document.downcase
              .scan(TOKEN_REGEX)
              .reject { |t| t =~ ONLY_DIGIT_REGEX }
    end

    def interestingeness(word)
      (0.5 - @spamicities[word]).abs
    end

    def combined_probability(probabilities)
      a = probabilities.reduce(:*)
      b = probabilities.map { |p| 1.0 - p }.reduce(:*)
      a / (a + b)
    end

end
