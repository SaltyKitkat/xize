const UNITS: &[u8; 6] = b"KMGTPE";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Type {
    Binary,
    Metric,
}

impl Type {
    fn base(&self) -> u64 {
        match self {
            Type::Binary => 1024,
            Type::Metric => 1000,
        }
    }
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Level {
    Human,
    Custom(u8),
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Scale {
    ty: Type,
    level: Level,
}

impl Default for Scale {
    fn default() -> Self {
        Self {
            ty: Type::Binary,
            level: Level::Human,
        }
    }
}

impl Scale {
    pub fn scale(&self, num: u64) -> String {
        if self.level == Level::Custom(0) {
            return format!("{}B", num);
        }
        let base = self.ty.base();
        let suffix = match self.ty {
            Type::Binary => "iB",
            Type::Metric => "B",
        };
        if self.level == Level::Human {
            let mut num = num;
            let mut cnt = 0;
            while num > base * 10 {
                num /= base;
                cnt += 1;
            }
            if num < base {
                return format!("{} {}{}", num, UNITS[cnt - 1] as char, suffix);
            } else {
                let num = num as f64 / base as f64;
                return format!("{:.1} {}{}", num, UNITS[cnt] as char, suffix);
            }
        }
        todo!()
    }
}
