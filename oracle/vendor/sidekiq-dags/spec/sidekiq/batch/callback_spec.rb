require 'spec_helper'

describe Sidekiq::Batch::Callback::Worker do
  describe '#perform' do
    let(:serialized_status) { Sidekiq::Batch::FinalStatusSnapshot.new('ABCD').serialized }

    it 'does not do anything if it cannot find the callback class' do
      subject.perform('SampleCallback', 'complete', {}, 'ABCD', 'EFGH', serialized_status)
    end

    it 'does not do anything if event is different from complete or success' do
      expect(SampleCallback).not_to receive(:new)
      subject.perform('SampleCallback', 'ups', {}, 'ABCD', 'EFGH', serialized_status)
    end

    it 'calls on_success if defined' do
      callback_instance = double('SampleCallback', on_success: true)
      expect(SampleCallback).to receive(:new).and_return(callback_instance)
      expect(callback_instance).to receive(:on_success)
        .with(instance_of(Sidekiq::Batch::FinalStatusSnapshot), {})
      subject.perform('SampleCallback', 'success', {}, 'ABCD', 'EFGH', serialized_status)
    end

    it 'calls on_complete if defined' do
      callback_instance = double('SampleCallback')
      expect(SampleCallback).to receive(:new).and_return(callback_instance)
      expect(callback_instance).to receive(:on_complete)
        .with(instance_of(Sidekiq::Batch::FinalStatusSnapshot), {})
      subject.perform('SampleCallback', 'complete', {}, 'ABCD', 'EFGH', serialized_status)
    end

    it 'calls specific callback if defined' do
      callback_instance = double('SampleCallback')
      expect(SampleCallback).to receive(:new).and_return(callback_instance)
      expect(callback_instance).to receive(:sample_method)
        .with(instance_of(Sidekiq::Batch::FinalStatusSnapshot), {})
      subject.perform('SampleCallback#sample_method', 'complete', {}, 'ABCD', 'EFGH', serialized_status)
    end
  end
end
