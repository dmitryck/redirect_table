require 'active_record'
require 'dotenv/load'
require 'byebug'

#
# ORM
#
ActiveRecord::Base.establish_connection \
  adapter:  'postgresql',
  host:     ENV['DBHOST'],
  username: ENV['DBUSER'],
  password: ENV['DBPASS'],
  database: ENV['DBNAME']

class Table < ActiveRecord::Base
  self.table_name = 'deleted_urls'
end

#
# tasks
#
desc 'Remove cycle links from table'
task :task1 do
  ActiveRecord::Base.connection.execute(<<-SQL)
    delete from deleted_urls
    where id in (
      select id
      from (
        select t2.id, link1, link2
        from (
          select id, link as link1, lag(id) over() target_id, lag(link) over() link2
          from (
            select id, (prev_url||next_url) as link, created_at
            from deleted_urls
            union (
              select id, (next_url||prev_url) as link, created_at
              from deleted_urls
            )
            order by link, created_at desc
          ) t1
        ) t2
        where t2.link1 = t2.link2
      ) t3
    )
  SQL
end

desc 'Replace cross links in table by last value'
task :task2 do
  RubyVM::InstructionSequence.compile_option = {
    tailcall_optimization: true,
    trace_instruction: false
  }

  #
  # alternative exit from recursion without rescue
  # fast variant too:
  # url, id = data.delete(url || (data.first&.send(:[], 0) || return))
  #
  # slow classic variant (add at method begining):
  # return if data.count == 0
  #
  RubyVM::InstructionSequence.new(<<-EOS).eval
    def worker(data, url, ids, result)
      url, id = data.delete(url || data.first[0]) rescue return
      data[url] ? ids << id : (result[url] = ids if ids.count > 10; ids = [])
      worker(data, url, ids, result)
    end
  EOS

  Table.find_each.each_with_object(data = {}) do |row, hash|
    hash[row.prev_url] = [row.next_url, row.id]
  end

  worker(data, nil, [], result = {})

  for url, ids in result
    Table.where(id: ids).update_all(next_url: url)
  end
end
