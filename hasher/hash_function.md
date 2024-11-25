
# Shrink Filter should use quick hash rather than slow hash

Although the complexity of hashing is O (1), 
the cost of one hash is still significant.


More hash function can be seen: https://github.com/prasanthj/hasher

### quick hash function
```c++
// See Martin Dietzfelbinger, "Universal hashing and k-wise independent random
// variables via integer arithmetic without primes".
class TwoIndependentMultiplyShift {
  unsigned __int128 multiply_, add_;

 public:
  TwoIndependentMultiplyShift() {
    ::std::random_device random;
    for (auto v : {&multiply_, &add_}) {
      *v = random();
      for (int i = 1; i <= 4; ++i) {
        *v = *v << 32;
        *v |= random();
      }
    }
  }

  uint64_t operator()(uint64_t key) const {
    return (add_ + multiply_ * static_cast<decltype(multiply_)>(key)) >> 64;
  }
};
```

### Java version
```java
import java.util.Random;

public class TwoIndependentMultiplyShift {
    private long multiply_;
    private long add_;

    public TwoIndependentMultiplyShift() {
        Random random = new Random();
        multiply_ = random.nextLong();
        add_ = random.nextLong();

        for (int i = 0; i < 4; i++) {
            multiply_ = (multiply_ << 32) | random.nextLong();
            add_ = (add_ << 32) | random.nextLong();
        }
    }

    public long apply(long key) {
        return (add_ + multiply_ * key) >>> 64;
    }
}
```

