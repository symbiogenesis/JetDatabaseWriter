namespace JetDatabaseReader.Tests;

using FluentAssertions;
using Xunit;

public class LruCacheTests
{
    [Fact]
    public void Add_And_TryGetValue_Returns_Stored_Value()
    {
        var cache = new LruCache<string, int>(4);

        cache.Add("a", 1);

        cache.TryGetValue("a", out int value).Should().BeTrue();
        value.Should().Be(1);
    }

    [Fact]
    public void TryGetValue_Missing_Key_Returns_False()
    {
        var cache = new LruCache<string, int>(4);

        cache.TryGetValue("missing", out _).Should().BeFalse();
    }

    [Fact]
    public void Count_Reflects_Number_Of_Entries()
    {
        var cache = new LruCache<string, int>(4);

        cache.Count.Should().Be(0);

        cache.Add("a", 1);
        cache.Add("b", 2);

        cache.Count.Should().Be(2);
    }

    [Fact]
    public void Add_Duplicate_Key_Updates_Value()
    {
        var cache = new LruCache<string, int>(4);

        cache.Add("a", 1);
        cache.Add("a", 42);

        cache.TryGetValue("a", out int value).Should().BeTrue();
        value.Should().Be(42);
        cache.Count.Should().Be(1);
    }

    [Fact]
    public void Evicts_Least_Recently_Used_When_At_Capacity()
    {
        var cache = new LruCache<string, int>(3);

        cache.Add("a", 1);
        cache.Add("b", 2);
        cache.Add("c", 3);

        // Adding a fourth entry should evict "a" (oldest).
        cache.Add("d", 4);

        cache.Count.Should().Be(3);
        cache.TryGetValue("a", out _).Should().BeFalse();
        cache.TryGetValue("b", out _).Should().BeTrue();
        cache.TryGetValue("c", out _).Should().BeTrue();
        cache.TryGetValue("d", out _).Should().BeTrue();
    }

    [Fact]
    public void TryGetValue_Promotes_Entry_So_It_Is_Not_Evicted()
    {
        var cache = new LruCache<string, int>(3);

        cache.Add("a", 1);
        cache.Add("b", 2);
        cache.Add("c", 3);

        // Access "a" to promote it to most-recently used.
        cache.TryGetValue("a", out _);

        // Adding "d" should now evict "b" (the new LRU) instead of "a".
        cache.Add("d", 4);

        cache.TryGetValue("a", out _).Should().BeTrue();
        cache.TryGetValue("b", out _).Should().BeFalse();
    }

    [Fact]
    public void Add_Existing_Key_Promotes_Entry()
    {
        var cache = new LruCache<string, int>(3);

        cache.Add("a", 1);
        cache.Add("b", 2);
        cache.Add("c", 3);

        // Re-add "a" with updated value — should promote it.
        cache.Add("a", 10);

        // Adding "d" should evict "b" (the LRU), not "a".
        cache.Add("d", 4);

        cache.TryGetValue("a", out int value).Should().BeTrue();
        value.Should().Be(10);
        cache.TryGetValue("b", out _).Should().BeFalse();
    }

    [Fact]
    public void Multiple_Evictions_Reuse_Slots_Correctly()
    {
        var cache = new LruCache<int, string>(2);

        cache.Add(1, "one");
        cache.Add(2, "two");

        // Evict 1, add 3
        cache.Add(3, "three");

        // Evict 2, add 4
        cache.Add(4, "four");

        cache.Count.Should().Be(2);
        cache.TryGetValue(1, out _).Should().BeFalse();
        cache.TryGetValue(2, out _).Should().BeFalse();
        cache.TryGetValue(3, out _).Should().BeTrue();
        cache.TryGetValue(4, out _).Should().BeTrue();
    }

    [Fact]
    public void Capacity_One_Always_Holds_Last_Added()
    {
        var cache = new LruCache<string, int>(1);

        cache.Add("a", 1);
        cache.Add("b", 2);

        cache.Count.Should().Be(1);
        cache.TryGetValue("a", out _).Should().BeFalse();
        cache.TryGetValue("b", out int value).Should().BeTrue();
        value.Should().Be(2);
    }

    [Fact]
    public void Works_With_Integer_Keys()
    {
        var cache = new LruCache<int, string>(3);

        cache.Add(100, "hundred");
        cache.Add(200, "two hundred");

        cache.TryGetValue(100, out string? v1).Should().BeTrue();
        v1.Should().Be("hundred");

        cache.TryGetValue(200, out string? v2).Should().BeTrue();
        v2.Should().Be("two hundred");
    }

    [Fact]
    public void Eviction_Order_Follows_Access_Pattern()
    {
        var cache = new LruCache<string, int>(3);

        cache.Add("a", 1);
        cache.Add("b", 2);
        cache.Add("c", 3);

        // Access order: c (most recent add), then access a, then b.
        cache.TryGetValue("a", out _);
        cache.TryGetValue("b", out _);

        // LRU order is now: c (least), a, b (most recent).
        // Adding "d" should evict "c".
        cache.Add("d", 4);

        cache.TryGetValue("c", out _).Should().BeFalse();
        cache.TryGetValue("a", out _).Should().BeTrue();
        cache.TryGetValue("b", out _).Should().BeTrue();
        cache.TryGetValue("d", out _).Should().BeTrue();
    }
}
