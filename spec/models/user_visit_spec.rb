require 'rails_helper'

describe UserVisit do
  let(:user) { Fabricate(:user) }
  let(:other_user) { Fabricate(:user) }

  describe '#by_day' do
    before(:each) do
      Timecop.freeze
      user.user_visits.create(visited_at: Time.now)
      user.user_visits.create(visited_at: 1.day.ago)
      other_user.user_visits.create(visited_at: 1.day.ago)
      user.user_visits.create(visited_at: 2.days.ago)
      user.user_visits.create(visited_at: 4.days.ago)
    end
    after(:each) { Timecop.return }
    let(:visits_by_day) { {1.day.ago.to_date => 2, 2.days.ago.to_date => 1, Time.now.to_date => 1 } }

    it 'collect closed interval visits' do
      expect(UserVisit.by_day(2.days.ago, Time.now)).to include(visits_by_day)
      expect(UserVisit.by_day(2.days.ago, Time.now)).not_to include({4.days.ago.to_date => 1})
    end
  end
end
